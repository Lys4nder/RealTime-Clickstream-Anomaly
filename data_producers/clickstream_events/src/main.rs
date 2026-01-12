use chrono::{DateTime, Utc};
use rand::{rng, Rng};
use serde::Serialize;
use std::thread;
use std::sync::Arc;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{BaseRecord, ThreadedProducer, DefaultProducerContext};
use std::env;
use uuid::Uuid;

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

fn random_sleep(low: u64, high: u64) {
    let mut rng = rng();
    let sleep_duration = rng.random_range(low..high);
    thread::sleep(std::time::Duration::from_millis(sleep_duration));
}

fn random_value<T: Clone>(values: &[T]) -> T {
    let mut rng = rng();
    values[rng.random_range(0..values.len())].clone()
}

fn weighted_choice<T: Clone, F: FnMut(u32) -> u32>(mut rand_fn: F, choices: &[(T, u32)]) -> T {
    let total: u32 = choices.iter().map(|(_, w)| *w).sum();
    let mut pick = if total == 0 { 0 } else { (rand_fn)(total) };
    for (item, w) in choices {
        if pick < *w {
            return item.clone();
        }
        pick -= *w;
    }
    choices[0].0.clone()
}

fn choose_action_for_page<F: FnMut(u32) -> u32>(rand_fn: F, page: PageCategory) -> ActionType {
    match page {
        PageCategory::Electronics => weighted_choice(rand_fn, &[
            (ActionType::ViewProduct, 50),
            (ActionType::AddToCart, 20),
            (ActionType::PageScroll, 15),
            (ActionType::ClickAd, 5),
            (ActionType::PurchaseComplete, 5),
        ]),
        PageCategory::WomensApparel | PageCategory::MensApparel | PageCategory::Accessories => {
            weighted_choice(rand_fn, &[
                (ActionType::ViewProduct, 50),
                (ActionType::ViewCategory, 20),
                (ActionType::AddToCart, 10),
                (ActionType::PageScroll, 15),
                (ActionType::ClickAd, 5),
            ])
        }
        PageCategory::HomeGoods | PageCategory::Clearance => weighted_choice(rand_fn, &[
            (ActionType::ViewCategory, 50),
            (ActionType::ViewProduct, 20),
            (ActionType::AddToCart, 10),
            (ActionType::PageScroll, 15),
        ]),
        PageCategory::AccountManagement | PageCategory::HomepageLanding => weighted_choice(rand_fn, &[
            (ActionType::PageScroll, 40),
            (ActionType::NavigationBack, 20),
            (ActionType::Search, 20),
            (ActionType::ClickAd, 10),
            (ActionType::ViewCategory, 10),
        ]),
    }
}

fn sample_session_length(rng: &mut impl Rng, max_events: usize) -> usize {
    // Probability of ending the session on each event (geometric): read from env or default 0.1
    let p = std::env::var("SESSION_END_PROB")
        .ok()
        .and_then(|s| s.parse::<f64>().ok())
        .filter(|v| *v > 0.0 && *v < 1.0)
        .unwrap_or(0.1);

    let mut count = 0usize;
    while count < max_events {
        count += 1;
        if rng.random_bool(p) {
            break;
        }
    }
    count
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
            let session_id = Uuid::new_v4().to_string();
            let is_suspicious = rng.random_range(1..=50) == 1;
            let user_id = format!("user_{}", rng.random_range(1000..9999));
            let ip_address = random_ip_address();
            let country = random_value(&CountryCodes::VARIANTS);
            let device_type = random_value(&DeviceType::VARIANTS);
            // decide sleep bounds once per thread/session
            let (sleep_low, sleep_high) = if is_suspicious {
                (50u64, 100u64)
            } else {
                (500u64, 2000u64)
            };

            // helper to build and send an event
            let send_event = |producer_ref: &ThreadedProducer<DefaultProducerContext>,
                              topic: &str,
                              key: &str,
                              event: &ClickEvent| {
                let json_string = serde_json::to_string(event).unwrap();
                if let Err((e, _)) = producer_ref.send(BaseRecord::to(topic).payload(&json_string).key(key)) {
                    eprintln!("Error sending message: {:?}", e);
                } else {
                    println!("Sent: {}", json_string);
                }
            };

            let session_events = sample_session_length(&mut rng, max_events);
            let mut current_product: Option<String> = None;
            let mut product_reuse_remaining: usize = 0;

            for idx in 0..session_events {
                let click_sequence = idx as u32 + 1;

                let page = random_value(&PageCategory::VARIANTS);
                let action = choose_action_for_page(|n| rng.random_range(0..n), page.clone());

                let product_code = match page {
                    PageCategory::Electronics
                    | PageCategory::WomensApparel
                    | PageCategory::MensApparel
                    | PageCategory::Accessories => {
                        if product_reuse_remaining == 0 {
                            let code = format!("PROD{}", rng.random_range(1000..9999));
                            current_product = Some(code.clone());
                            product_reuse_remaining = rng.random_range(1..=3);
                            // consume one immediately
                            product_reuse_remaining = product_reuse_remaining.saturating_sub(1);
                            code
                        } else {
                            product_reuse_remaining = product_reuse_remaining.saturating_sub(1);
                            current_product.clone().unwrap_or_else(|| format!("PROD{}", rng.random_range(1000..9999)))
                        }
                    }
                    _ => format!("PROD{}", rng.random_range(1000..9999)),
                };
                let event = ClickEvent {
                    event_timestamp: Utc::now(),
                    session_id: session_id.clone(),
                    user_id: user_id.clone(),
                    ip_address: ip_address.clone(),
                    country: country.clone(),
                    click_sequence,
                    page_category: page,
                    product_code,
                    action_type: action,
                    device_type: device_type.clone(),
                    page_section: random_value(&PageSection::VARIANTS),
                };

                send_event(producer.as_ref(), &topic, &user_id, &event);
                random_sleep(sleep_low, sleep_high);
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