use std::str::FromStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LoadBalancingMode {
    LeastConn,
    Queue,
}

impl LoadBalancingMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            LoadBalancingMode::LeastConn => "leastconn",
            LoadBalancingMode::Queue => "queue",
        }
    }
}

impl FromStr for LoadBalancingMode {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "leastconn" | "least_conn" | "least" => Ok(LoadBalancingMode::LeastConn),
            "queue" | "queued" => Ok(LoadBalancingMode::Queue),
            other => Err(format!("unsupported load balancer: {other}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn load_balancing_mode_parses_variants() {
        assert_eq!("leastconn".parse::<LoadBalancingMode>().unwrap(), LoadBalancingMode::LeastConn);
        assert_eq!("least_conn".parse::<LoadBalancingMode>().unwrap(), LoadBalancingMode::LeastConn);
        assert_eq!("least".parse::<LoadBalancingMode>().unwrap(), LoadBalancingMode::LeastConn);
        assert_eq!("queue".parse::<LoadBalancingMode>().unwrap(), LoadBalancingMode::Queue);
        assert_eq!("queued".parse::<LoadBalancingMode>().unwrap(), LoadBalancingMode::Queue);
        assert!("unknown".parse::<LoadBalancingMode>().is_err());
    }

    #[test]
    fn load_balancing_mode_as_str() {
        assert_eq!(LoadBalancingMode::LeastConn.as_str(), "leastconn");
        assert_eq!(LoadBalancingMode::Queue.as_str(), "queue");
    }
}
