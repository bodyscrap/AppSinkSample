use gst;
use gst::prelude::*;
use gst_app;
use gst_app::{AppSink, AppSinkCallbacks};
use std::fs::OpenOptions;
use csv::WriterBuilder;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicU64, Ordering};

const VIDEO_PATH: &str = "E:/sample.mp4"; // 動画のパスを指定
const CSV_PATH: &str = "output.csv"; // 出力CSVファイルのパス

fn main() -> Result<(), Box<dyn std::error::Error>> {
    gst::init()?;

    // パイプラインを作成
    let pipeline = gst::parse::launch(&format!(
        "filesrc location={} ! decodebin ! videoconvert ! videoscale ! video/x-raw,format=RGB ! appsink name=sink",
        VIDEO_PATH
    ))?;

    // AppSinkの取得
    let appsink = pipeline
        .clone()
        .dynamic_cast::<gst::Pipeline>()
        .expect("Pipeline cast failed")
        .by_name("sink")
        .expect("Sink not found")
        .dynamic_cast::<AppSink>()
        .expect("Sink element is not an AppSink");
    
    // CSVファイルの書き込み準備
    let file = OpenOptions::new()
        .write(true)
        .append(true)
        .create(true)
        .open(CSV_PATH)?;
    let writer = WriterBuilder::new().has_headers(false).from_writer(file);
    // 非同期アクセスできるようにArc<Mutex<Writer<File>>>でラップ
    let writer = Arc::new(Mutex::new(writer));

    // フレームカウンタ
    let frame_counter = Arc::new(AtomicU64::new(0));

    // AppSinkの設定
    appsink.set_callbacks(
        AppSinkCallbacks::builder()
            .new_sample(move |appsink| {
                let sample = appsink.pull_sample().expect("Failed to pull sample");
                let buffer = sample.buffer().expect("Failed to get buffer");

                // バッファのマッピング
                let map = buffer.map_readable().expect("Failed to map buffer");
                let data = map.as_slice();

                // RGB各プレーンの平均輝度を計算
                let (mut sum_r, mut sum_g, mut sum_b) = (0u64, 0u64, 0u64);
                let num_pixels = data.len() / 3;
                for chunk in data.chunks(3) {
                    sum_r += chunk[0] as u64;
                    sum_g += chunk[1] as u64;
                    sum_b += chunk[2] as u64;
                }
                let avg_r = (sum_r / num_pixels as u64) as u8;
                let avg_g = (sum_g / num_pixels as u64) as u8;
                let avg_b = (sum_b / num_pixels as u64) as u8;

                // フレーム番号を取得してインクリメント
                let frame_number = frame_counter.fetch_add(1, Ordering::SeqCst);

                // MutexからWriterを取得
                let mut writer = writer.lock().expect("Failed to lock writer");
                // CSVファイルに書き込む
                writer
                    .write_record(&[
                        frame_number.to_string(),
                        avg_r.to_string(),
                        avg_g.to_string(),
                        avg_b.to_string(),
                    ])
                    .expect("Failed to write record");
                writer.flush().expect("Failed to flush CSV writer");

                Ok(gst::FlowSuccess::Ok)
            })
            .build(),
    );

    // パイプラインを再生開始
    pipeline.set_state(gst::State::Playing)?;

    // 終了するまで待機
    let bus = pipeline.bus().unwrap();
    for msg in bus.iter_timed(gst::ClockTime::NONE) {
        use gst::MessageView;

        match msg.view() {
            MessageView::Eos(..) => break,
            MessageView::Error(err) => {
                eprintln!("Error  {:?}", err);
                break;
            }
            _ => (),
        }
    }

    // パイプラインを停止
    pipeline.set_state(gst::State::Null)?;

    Ok(())
}
