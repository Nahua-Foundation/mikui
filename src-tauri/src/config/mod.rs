pub mod secrets;
mod store;
mod types;

// `config_dir`/`read_json`/`write_atomic` торчат наружу ради `crate::proto`:
// схемы топиков живут в том же каталоге и по тем же правилам — атомарная
// запись, битый файл отодвигается в `.bad`. Заводить второй набор этих
// примитивов значило бы завести и второй набор способов их сломать.
pub use store::{
    clusters_at, config_dir, load_clusters, load_settings, read_json, save_clusters, save_settings,
    write_atomic,
};
pub use types::{ClusterConfig, ClusterUser, SchemaRegistry, Settings};
