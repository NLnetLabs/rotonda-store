#[macro_export]
#[doc(hidden)]
macro_rules! all_strategies {
    ( $( $fn_name: ident; $test_name: ident; $ty: ty ), * ) => {

        $(
            #[test]
            fn $fn_name() -> Result<(), Box<dyn std::error::Error>> {
                use rotonda_store::prelude::multi::*;
                //------- Default (MemoryOnly)
                println!("MemoryOnly strategy starting...");
                let tree_bitmap =
                    MultiThreadedStore::<
                        $ty, MemoryOnlyConfig>::try_default()?;

                $test_name(tree_bitmap)?;

                //------- PersistOnly

                println!("PersistOnly strategy starting...");
                let mut store_config = PersistOnlyConfig::default();
                store_config.set_persist_path(
                    "/tmp/rotonda/".into()
                );

                let tree_bitmap = MultiThreadedStore::<
                    $ty, PersistOnlyConfig
                >::new_with_config(
                    store_config
                )?;

                $test_name(tree_bitmap)?;

                //------- PersistHistory

                println!("PersistHistory strategy starting...");
                let mut store_config = PersistHistoryConfig::default();
                store_config.set_persist_path(
                    "/tmp/rotonda/".into()
                );

                let tree_bitmap = MultiThreadedStore::<
                    $ty,
                    PersistHistoryConfig
                >::new_with_config(
                    store_config
                )?;

                $test_name(tree_bitmap)?;

                //------- WriteAhead

                println!("WriteAhead strategy starting...");

                let mut store_config = WriteAheadConfig::default();
                store_config.set_persist_path(
                    "/tmp/rotonda/".into()
                );

                let tree_bitmap = MultiThreadedStore::<
                    $ty,
                    WriteAheadConfig
                >::new_with_config(
                    store_config
                )?;

                $test_name(tree_bitmap)?;

                Ok(())
            }
        )*
    };
}
