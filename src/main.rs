use anyhow::Error;
use futures::compat::Future01CompatExt;
use rdkafka::{admin, config};
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
#[structopt(name = "kafkaman", about = "Manage kafka")]
struct Opts {
    #[structopt(subcommand)]
    cmd: SubOpts,

    #[structopt(long, short, default_value = "localhost:9092")]
    broker: String,
}

#[derive(StructOpt, Debug)]
enum SubOpts {
    CreateTopic {
        name: String,
        #[structopt(short, long, default_value = "1")]
        partitions: u32,
        #[structopt(short, long, default_value = "1")]
        replication_factor: u32,
    },

    Echo {
        topic: String,
    },

    Produce,
}

fn main() -> Result<(), Error> {
    env_logger::init();

    let opts = Opts::from_args();
    log::trace!("{:?}", opts);

    let mut rt = tokio::runtime::Runtime::new()?;

    rt.block_on(async { KafkaMan::new(opts).run().await })
}

struct KafkaMan {
    opts: Opts,
}

impl KafkaMan {
    fn new(opts: Opts) -> Self {
        Self { opts }
    }

    async fn run(self) -> Result<(), Error> {
        log::info!("running kafkaman");
        match self.opts.cmd {
            SubOpts::CreateTopic {
                name,
                partitions,
                replication_factor,
            } => {
                log::info!("creating topic `{}`", name);

                let client: admin::AdminClient<_> = config::ClientConfig::new()
                    .set("bootstrap.servers", &self.opts.broker)
                    .set("session.timeout.ms", "6000")
                    .create()?;
                // TODO: timeouts
                let opts = admin::AdminOptions::new();
                log::debug!(
                    "creating topic {} with {} partitions and a replication factor of {}",
                    name,
                    partitions,
                    replication_factor
                );

                let new_topic = admin::NewTopic::new(
                    name.as_str(),
                    partitions as _,
                    admin::TopicReplication::Fixed(replication_factor as _),
                );

                let topic_results = client.create_topics(&[new_topic], &opts).compat().await?;
                for result in topic_results {
                    log::trace!("topic result {:?}", result);
                }

                Ok(())
            }
            _ => todo!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::compat::Future01CompatExt;
    use rdkafka::config::ClientConfig;
    use rdkafka::{admin, client, types, util};

    fn init() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    async fn kafkaman_main(opts: Opts) -> Result<(), Error> {
        KafkaMan::new(opts).run().await
    }

    async fn ensure_topic_removed(topic_name: &str) -> Result<(), Error> {
        let client: admin::AdminClient<_> = ClientConfig::new()
            .set("bootstrap.servers", "localhost:9092")
            .set("session.timeout.ms", "6000")
            .create()?;
        let opts = admin::AdminOptions::new();
        log::debug!("deleting topics");
        client.delete_topics(&[topic_name], &opts).compat().await?;
        log::debug!("finished deleting topics");
        Ok(())
    }

    fn list_topics() -> Result<Vec<String>, Error> {
        let config = ClientConfig::new()
            .set("bootstrap.servers", "localhost:9092")
            .set("session.timeout.ms", "6000")
            .to_owned();
        let native_config = config.create_native_config()?;
        let client = client::Client::new(
            &config,
            native_config,
            types::RDKafkaType::RD_KAFKA_PRODUCER,
            client::DefaultClientContext,
        )?;

        log::debug!("listing topics");
        let metadata = client.fetch_metadata(None, util::Timeout::Never)?;
        let topics = metadata.topics();
        Ok(topics.iter().map(|t| t.name().to_string()).collect())
    }

    #[tokio::test]
    async fn create_topic() {
        init();

        log::warn!("debug");

        let topic_name = "foo";
        ensure_topic_removed(topic_name).await.unwrap();

        let opts = Opts {
            cmd: SubOpts::CreateTopic {
                name: topic_name.into(),
                partitions: 1,
                replication_factor: 1,
            },
            broker: "localhost:9092".into(),
        };

        kafkaman_main(opts).await.unwrap();

        let topics = list_topics().unwrap();
        assert!(
            topics.contains(&topic_name.to_string()),
            "cannot find topic `{}` in topics {:?}",
            topic_name,
            topics
        );
    }
}
