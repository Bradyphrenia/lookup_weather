use postgres::{Client, NoTls};
use std::collections::HashMap;
use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    let (db_host, database, db_user, db_pass) = ("localhost", "wetter", "postgres", "postgres");
    let connection_string = format!(
        "host={} user={} password={} dbname={}",
        db_host, db_user, db_pass, database
    );
    let mut data: HashMap<String, f64> = HashMap::new();
    let mut client = Client::connect(&connection_string, NoTls)?;
    let query =
        "SELECT \"Time\"::TEXT as time, \"BME280_pressure\" as pressure FROM \"Feinstaubsensor\";";
    let rows = client.query(query, &[])?;
    for row in rows {
        let date_opt: Option<String> = row.get("time");
        let pressure_opt: Option<String> = row.get("pressure");
        let parsed_pressure = pressure_opt.and_then(|p| p.parse::<f64>().ok());
        if let Some((date, pressure)) = date_opt.zip(parsed_pressure) {
            let real_pressure = pressure / 100.0;
            data.insert(date, real_pressure);
        }
    }
    let query = "SELECT \"Zeit\"::TEXT as time, \"Abs. Luftdruck(hpa)\" as pressure FROM \"Wetterstation\";";
    let rows = client.query(query, &[])?;
    for row in rows {
        let date_opt: Option<String> = row.get("time");
        let pressure_opt: Option<f64> = row.get("pressure");
        if let Some((date, _pressure)) = date_opt.zip(pressure_opt) {
            if pressure_opt.unwrap() < 100.0 {
                print!("{} -> ", &date);
                let date_str = &date[..10];
                let timeline = make_timeline(&data, date_str);
                for (time, pressure) in timeline {
                    if is_same_hour(&time, &date[11..19]) && is_same_minute(&time, &date[11..19]) {
                        println!("{}: {}", time, pressure);
                        break;
                    }
                }
            }
        }
    }
    Ok(())
}

fn filter_date(date: &str, date_str: &str) -> bool {
    date.starts_with(date_str)
}

fn make_timeline(data: &HashMap<String, f64>, date_str: &str) -> HashMap<String, String> {
    let mut timeline: HashMap<String, String> = HashMap::new();
    for (date, pressure) in data {
        if filter_date(date, date_str) {
            let time = &date[11..19];
            timeline.insert(time.to_string(), pressure.to_string());
        }
    }
    timeline
}

fn is_same_hour(t1: &str, t2: &str) -> bool {
    // Compares "HH", ignores ":MM:SS"
    t1.get(0..2) == t2.get(0..2)
}

fn is_same_minute(t1: &str, t2: &str) -> bool {
    // Compares "MM", ignores ":SS"
    if (t1.get(3..5) >= t2.get(3..5)) | ((t2.get(3..5) >= Some("57")) && t1.get(3..5) >= Some("54"))
    {
        true
    } else {
        false
    }
}
