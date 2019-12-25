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
        #[structopt(short, long)]
        replication_factor: u32,
    },

    Echo {
        topic: String,
    },

    Produce,
}

fn main() {
    env_logger::init();

    let opts = Opts::from_args();
    log::trace!("{:?}", opts);
}
