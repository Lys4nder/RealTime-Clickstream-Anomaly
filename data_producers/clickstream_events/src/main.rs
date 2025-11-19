use chrono::{DateTime, Utc};
use rand::{rng, Rng};
use serde::Serialize;
use std::io;
use std::thread;

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
        rng.gen_range(1..=255),
        rng.gen_range(0..=255),
        rng.gen_range(0..=255),
        rng.gen_range(1..=255)
    )
}

fn random_sleep() {
    let mut rng = rng();
    let sleep_duration = rng.gen_range(100..500);
    thread::sleep(std::time::Duration::from_millis(sleep_duration));
}

fn random_value<T: Clone>(values: &[T]) -> T {
    let mut rng = rng();
    values[rng.gen_range(0..values.len())].clone()
}

fn main() {
    println!("How many threads do you want to spawn? (press Enter for default: 1)");

    let num_threads: usize = loop {
        let mut buffer = String::new();
        io::stdin().read_line(&mut buffer).expect("Failed to read line");
        let input = buffer.trim();

        if input.is_empty() {
            println!("No input received; defaulting to 1 thread.");
            break 1usize;
        }

        match input.parse::<usize>() {
            Ok(n) if n > 0 => break n,
            _ => {
                println!("Please enter a positive integer (or press Enter for default):");
                continue;
            }
        }
    };

    println!("Starting clickstream event producer...");

    let mut handles = Vec::new();

    for thread_id in 0..num_threads {
        let handle = thread::spawn(move || {
            let mut rng = rng();
            let mut click_sequence = 0u32;

            let session_id = format!("session_{}", thread_id); //TODO improve session id generation
            let user_id = format!("user_{}", rng.gen_range(1000..9999)); //TODO improve user id generation

            loop {
                let event = ClickEvent {
                    event_timestamp: Utc::now(),
                    session_id: session_id.clone(),
                    user_id: user_id.clone(),
                    ip_address: random_ip_address(),
                    country: random_value(&CountryCodes::VARIANTS),
                    click_sequence,
                    page_category: random_value(&PageCategory::VARIANTS),
                    product_code: format!("PROD{}", rng.gen_range(1000..9999)),
                    action_type: random_value(&ActionType::VARIANTS),
                    device_type: random_value(&DeviceType::VARIANTS),
                    page_section: random_value(&PageSection::VARIANTS),
                };

                println!("{}", serde_json::to_string(&event).unwrap());
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
