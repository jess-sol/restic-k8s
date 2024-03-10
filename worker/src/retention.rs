use std::{collections::BTreeMap, str::FromStr};

// Note - Copied from operator; ensure kept up to date. Consider splitting into separate small
// library to share code instead.
#[derive(Clone, Debug)]
pub struct RetentionSpec {
    pub hourly: Option<u32>,
    pub daily: Option<u32>,
    pub weekly: Option<u32>,
    pub monthly: Option<u32>,
    pub yearly: Option<u32>,
}

impl RetentionSpec {
    fn fields(&self) -> [(&str, Option<u32>); 5] {
        [
            ("hourly", self.hourly),
            ("daily", self.daily),
            ("weekly", self.weekly),
            ("monthly", self.monthly),
            ("yearly", self.yearly),
        ]
    }

    pub(crate) fn as_args(&self) -> Vec<String> {
        let mut values = Vec::new();

        for (name, value) in self.fields() {
            if let Some(value) = value {
                values.push(format!("--keep-{name}={value}"));
            }
        }

        values
    }
}

impl FromStr for RetentionSpec {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let mut values = BTreeMap::new();
        let fields = s.split(',');
        for field in fields {
            let mut iter = field.rsplitn(2, '=');
            match (iter.next(), iter.next()) {
                (Some(v), Some(n)) => {
                    let v: u32 = v.parse().map_err(|x| format!("Unable to parse {n}: {x}"))?;
                    values.insert(n, v);
                }
                (Some(x), _) => return Err(format!("Missing value for field {x}")),
                (None, _) => continue,
            }
        }

        Ok(Self {
            hourly: values.remove("hourly"),
            daily: values.remove("daily"),
            weekly: values.remove("weekly"),
            monthly: values.remove("monthly"),
            yearly: values.remove("yearly"),
        })
    }
}

impl ToString for RetentionSpec {
    fn to_string(&self) -> String {
        let mut values = Vec::new();

        for (name, value) in self.fields() {
            if let Some(value) = value {
                values.push(format!("{name}={value}"));
            }
        }

        values.join(",")
    }
}
