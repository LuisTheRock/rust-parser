pub mod pb {
    tonic::include_proto!("hostdirectory.v1");
}

use pb::FetchHostsRequest;
use pb::host_directory_client::HostDirectoryClient;
use tonic::transport::Channel;

pub struct HostDirectoryClientHandle {
    client: HostDirectoryClient<Channel>,
    address: String,
}

impl HostDirectoryClientHandle {
    pub async fn connect(address: &str) -> anyhow::Result<Self> {
        let client = HostDirectoryClient::connect(address.to_string()).await?;
        tracing::info!(address, "host-directory grpc connection established");
        Ok(Self {
            client,
            address: address.to_string(),
        })
    }

    pub async fn fetch_active_hosts(&mut self) -> anyhow::Result<Vec<(String, String)>> {
        let response = self
            .client
            .fetch_hosts(FetchHostsRequest {
                has_board: Some(true),
            })
            .await?;

        let pairs = response
            .into_inner()
            .hosts
            .into_iter()
            .flat_map(|host| {
                let host_id = host.host_id;
                host.boards
                    .into_iter()
                    .filter(is_active_board)
                    .map(move |board| (board.imei, host_id.clone()))
            })
            .collect();

        Ok(pairs)
    }
}

impl Drop for HostDirectoryClientHandle {
    fn drop(&mut self) {
        tracing::info!(address = %self.address, "host-directory grpc connection closing");
    }
}

fn is_active_board(board: &pb::Board) -> bool {
    match &board.effective_to {
        None => true,
        Some(ts) => ts.seconds == 0 && ts.nanos == 0,
    }
}
