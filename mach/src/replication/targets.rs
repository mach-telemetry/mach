use dashmap::DashMap;
use std::{
    sync::Arc,
    time::Duration,
    thread,
    net::{IpAddr, SocketAddr},
    collections::HashMap,
    env,
    str::FromStr,
};
use crate::{
    id::SeriesId,
    series::Series,
    constants::*,
};
use kube::{api::{Api, ListParams, ResourceExt}, Client};
use k8s_openapi::api::core::v1::*;
use consistent_hash_ring::{Ring, RingBuilder};

async fn get_addresses() -> HashMap<String, SocketAddr> {
    // Get host list
    let client = Client::try_default().await.unwrap();
    let pods: Api<Pod> = Api::namespaced(client, KUBE_NAMESPACE);
    let lp = ListParams::default();
    let mut hosts = HashMap::new();
    for p in pods.list(&lp).await.unwrap() {
        let name = p.name().clone();
        let address = {
            let status = p.status.unwrap().clone();
            let addr = status.pod_ip.unwrap().clone();
            SocketAddr::new(IpAddr::from_str(&addr).unwrap(), KUBE_PORT)
        };
        hosts.insert(name, address);
    }
    hosts
}

pub fn get_replicas() -> Vec<SocketAddr> {
    let this_host: String = env::var("HOSTNAME").expect("K8s pod hostname not available").into();
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    let mut addresses = rt.block_on(get_addresses());

    let this_address = addresses.remove(&this_host).unwrap();

    // Ring and get two replicas
    let address_iter = addresses.iter().map(|(k, v)| v.clone());
    let ring = RingBuilder::default().nodes_iter(address_iter).build();
    ring.replicas(&this_address).take(2).cloned().collect::<Vec<SocketAddr>>()
}
