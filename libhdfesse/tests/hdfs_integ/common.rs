use libhdfesse::hdconfig::{Config, NamenodeConfig, NameserviceConfig};

pub(crate) const HADOOP_HOST: &str = "hadoop";
pub(crate) const HADOOP_PORT: u16 = 9000;
pub(crate) const HADOOP_DEFAULT: &str = "default2";

/** Construct default test config.  We do not use auto config for
 * tests that modify HDFS tree, as it can be harmful when run out of
 * docker container.  We use hardcoded config instead.
 *
 * Please note that it has to match config inside docker, as
 * we run test for their comparison.
 */
pub(crate) fn get_default_config() -> Config {
    Config {
        default_fs: Some(format!("hdfs://{}", HADOOP_DEFAULT).into()),
        services: vec![NameserviceConfig {
            name: HADOOP_DEFAULT.into(),
            rpc_nodes: vec![NamenodeConfig {
                name: "nn".into(),
                rpc_address: format!("{}:{}", HADOOP_HOST, HADOOP_PORT).into(),
                servicerpc_address: format!("{}:{}", HADOOP_HOST, HADOOP_PORT).into(),
            }],
        }],
    }
}
