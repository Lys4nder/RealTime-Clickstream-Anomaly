use chrono::{DateTime, Utc};
use rand::{rng, Rng};
use serde::Serialize;
use std::thread;
use std::sync::Arc;
mod handle_parquet;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{BaseRecord, ThreadedProducer, DefaultProducerContext};
use std::env;

#[derive(Debug, Serialize, Clone)]
struct ClickEvent {
    event_timestamp: DateTime<Utc>,
    session_id: String,
    user_id: String,
    ip_address: String,
    country: CountryCodes,
    click_sequence: u32,
    page_category: PageCategory,
    product_code: String,
    action_type: ActionType,
    device_type: DeviceType,
    page_section: PageSection,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum PageCategory {
    WomensApparel,
    MensApparel,
    Accessories,
    Electronics,
    HomeGoods,
    Clearance,
    AccountManagement,
    HomepageLanding,
}

impl PageCategory {
    const VARIANTS: [PageCategory; 8] = [
        PageCategory::WomensApparel,
        PageCategory::MensApparel,
        PageCategory::Accessories,
        PageCategory::Electronics,
        PageCategory::HomeGoods,
        PageCategory::Clearance,
        PageCategory::AccountManagement,
        PageCategory::HomepageLanding,
    ];
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum ActionType {
    ViewProduct,
    ViewCategory,
    AddToCart,
    RemoveFromCart,
    Search,
    CheckoutStart,
    PurchaseComplete,
    PageScroll,
    ClickAd,
    NavigationBack,
}

impl ActionType {
    const VARIANTS: [ActionType; 10] = [
        ActionType::ViewProduct,
        ActionType::ViewCategory,
        ActionType::AddToCart,
        ActionType::RemoveFromCart,
        ActionType::Search,
        ActionType::CheckoutStart,
        ActionType::PurchaseComplete,
        ActionType::PageScroll,
        ActionType::ClickAd,
        ActionType::NavigationBack,
    ];
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum DeviceType {
    Mobile,
    Desktop,
    Tablet,
}

impl DeviceType {
    const VARIANTS: [DeviceType; 3] = [
        DeviceType::Mobile,
        DeviceType::Desktop,
        DeviceType::Tablet,
    ];
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum PageSection {
    TopBanner,
    MiddleGrid,
    BottomRecommendations,
    Sidebar,
    ModalPopup,
}

impl PageSection {
    const VARIANTS: [PageSection; 5] = [
        PageSection::TopBanner,
        PageSection::MiddleGrid,
        PageSection::BottomRecommendations,
        PageSection::Sidebar,
        PageSection::ModalPopup,
    ];
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum CountryCodes {
    US,
    CA,
    GB,
    DE,
    FR,
    IN,
    AU,
    JP,
    BR,
    ZA,
}

impl CountryCodes {
    const VARIANTS: [CountryCodes; 10] = [
        CountryCodes::US,
        CountryCodes::CA,
        CountryCodes::GB,
        CountryCodes::DE,
        CountryCodes::FR,
        CountryCodes::IN,
        CountryCodes::AU,
        CountryCodes::JP,
        CountryCodes::BR,
        CountryCodes::ZA,
    ];
}

fn random_ip_address() -> String {
    let mut rng = rng();
    format!(
        "{}.{}.{}.{}",
        rng.random_range(1..=255),
        rng.random_range(0..=255),
        rng.random_range(0..=255),
        rng.random_range(1..=255)
    )
}

fn random_sleep() {
    let mut rng = rng();
    let sleep_duration = rng.random_range(100..500);
    thread::sleep(std::time::Duration::from_millis(sleep_duration));
}

fn random_value<T: Clone>(values: &[T]) -> T {
    let mut rng = rng();
    values[rng.random_range(0..values.len())].clone()
}

fn generate(producer: Arc<ThreadedProducer<DefaultProducerContext>>, topic_name: String, max_events: usize) {

    let num_threads = env::var("NUM_THREADS")
            .unwrap_or_else(|_| "1".to_string())
            .parse::<usize>()
            .unwrap_or(1);

    println!("Starting clickstream event producer with {} threads...", num_threads);

    let mut handles = Vec::new();

    for thread_id in 0..num_threads {
        let producer = Arc::clone(&producer);
        let topic = topic_name.clone();

        let handle = thread::spawn(move || {
            let mut rng = rng();
            let mut click_sequence = 0u32;
            let session_id = format!("session_{}", thread_id);
            let user_id = format!("user_{}", rng.random_range(1000..9999));

            for _ in 0..max_events {
                let event = ClickEvent {
                    event_timestamp: Utc::now(),
                    session_id: session_id.clone(),
                    user_id: user_id.clone(),
                    ip_address: random_ip_address(),
                    country: random_value(&CountryCodes::VARIANTS),
                    click_sequence,
                    page_category: random_value(&PageCategory::VARIANTS),
                    product_code: format!("PROD{}", rng.random_range(1000..9999)),
                    action_type: random_value(&ActionType::VARIANTS),
                    device_type: random_value(&DeviceType::VARIANTS),
                    page_section: random_value(&PageSection::VARIANTS),
                };

                let json_string = serde_json::to_string(&event).unwrap();

                if let Err((e, _)) = producer.send(
                    BaseRecord::to(&topic)
                        .payload(&json_string)
                        .key(&user_id),
                ) {
                    eprintln!("Error sending message: {:?}", e);
                } else {
                    println!("Sent: {}", json_string);
                }

                click_sequence += 1;
                random_sleep();
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }
}

fn main() {
    env_logger::init();

    // Read Env Vars
    let broker = env::var("KAFKA_BROKER").unwrap_or_else(|_| "localhost:9092".to_string());
    let topic = env::var("KAFKA_TOPIC").expect("KAFKA_TOPIC must be set");
    let max_events = env::var("MAX_EVENTS")
        .unwrap_or_else(|_| "1000".to_string())
        .parse::<usize>()
        .unwrap_or(1000);

    println!("Connecting to Kafka at: {}", broker);
    println!("Max events per thread: {}", max_events);

    let producer: ThreadedProducer<DefaultProducerContext> = ClientConfig::new()
        .set("bootstrap.servers", &broker)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation error");

    let producer = Arc::new(producer);
    generate(producer, topic, max_events);
}