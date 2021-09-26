use libhdfesse::hdconfig::Config;

#[test]
fn test_auto_config() {
    assert_eq!(Config::auto(), crate::common::get_default_config())
}
