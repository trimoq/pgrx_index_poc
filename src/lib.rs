use std::os::raw::c_void;
use std::ptr::null_mut;

use pg_sys::{Datum, InvalidOid};
use pgrx::prelude::*;
use pgrx::pg_sys::*;
use pgrx::callconv::{FcInfo, RetAbi, CallCx};
use pgrx::PgMemoryContexts;

::pgrx::pg_module_magic!();




#[pg_extern]
fn foo() -> &'static str {
    warning!("much foo");
    "uhhhhh"
}


fn foohandler(_info: FunctionCallInfo) -> PgBox<IndexAmRoutine,AllocatedByRust> {
    // warning!("Setting up the foohandler!");
    let am =  get_default_IAMR();
    let mut am_ptr = unsafe { PgBox::<IndexAmRoutine>::alloc_node(NodeTag::T_IndexAmRoutine) };
    *am_ptr = am;
    am_ptr
}



#[allow(non_snake_case)]
fn get_default_IAMR()->IndexAmRoutine{
    IndexAmRoutine{
        type_: pgrx::pg_sys::NodeTag::T_IndexAmRoutine,
        amstrategies: 1,
        amsupport: 2,
        amoptsprocnum: 2,
        amcanorder: false,
        amcanorderbyop: false,
        amcanbackward: false,
        amcanunique: false,
        amcanmulticol: true,
        amoptionalkey: true,
        amsearcharray: false,
        amsearchnulls: false,
        amstorage: false,
        amclusterable: false,
        ampredlocks: false,
        amcanparallel: false,
        amcaninclude: false,
        amusemaintenanceworkmem: false,
        amsummarizing: false,
        amparallelvacuumoptions: 1,
        amkeytype: InvalidOid,
        ambuild: Some(foo_build),
        ambuildempty: Some(foo_build_empty),
        aminsert: Some(foo_insert),
        ambulkdelete: Some(foo_bulk_delete),
        amvacuumcleanup: Some(foo_vacuum_cleanup),
        amcanreturn: None,
        amcostestimate: Some(foo_cost_estimate),
        amoptions: Some(foo_options),
        amproperty: None,
        ambuildphasename: None,
        amvalidate: Some(foo_validate),
        amadjustmembers: None,
        ambeginscan: Some(foo_begin_scan),
        amrescan: Some(foo_rescan),
        amgettuple: None,
        amgetbitmap: Some(foo_bitmap),
        amendscan: Some(foo_end_scan),
        ammarkpos: None,
        amrestrpos: None,
        amestimateparallelscan: None,
        aminitparallelscan: None,
        amparallelrescan: None,
    }
}


#[allow(non_snake_case)]
unsafe extern "C" fn foo_build(
    heapRelation: Relation,
    indexRelation: Relation,
    indexInfo: *mut IndexInfo,
) -> *mut IndexBuildResult{
    warning!("Creating a foo index!");

    let num_blocks_in_index = RelationGetNumberOfBlocksInFork(
        indexRelation, 0.into()
    );

    unsafe extern "C" fn fooBuildCallback(
        _index: Relation,
        tid: ItemPointer,
        _values: *mut Datum,
        _isnull: *mut bool,
        _tupleIsAlive: bool,
        _state: *mut ::core::ffi::c_void,
    ){
        warning!("Called fooBuildCallback, ip_blkid: {:?} {}", (*tid).ip_blkid, (*tid).ip_posid);
    }

    let scan = (*(*heapRelation).rd_tableam).index_build_range_scan.unwrap();
    let numTuples = unsafe{
        scan(
            heapRelation, indexRelation, indexInfo, true, false, true, 0, InvalidBlockNumber, 
            Some(fooBuildCallback),
            null_mut::<c_void>().into(),
            null_mut::<TableScanDescData>().into()
        )
    };
    
    let heap_tuples = 0;
    let index_tuples = 0;

    warning!("Foo index creation: num_blocks_in_index:{num_blocks_in_index}, numTuples:{numTuples}");


    let mut res = unsafe { PgBox::<pgrx::pg_sys::IndexBuildResult>::alloc() };



    *res = IndexBuildResult{
        heap_tuples: heap_tuples as f64,
        index_tuples: index_tuples as f64,
    };


    res.into_pg()
}


#[allow(non_snake_case)]
unsafe extern "C" fn foo_build_empty(
    _indexRelation: Relation
){
    warning!("Creating n empty foo index!");
}

#[allow(non_snake_case)]
unsafe extern "C" fn foo_insert(
    _indexRelation: Relation,
    _values: *mut Datum,
    _isnull: *mut bool,
    _heap_tid: ItemPointer,
    _heapRelation: Relation,
    _checkUnique: IndexUniqueCheck::Type,
    _indexUnchanged: bool,
    _indexInfo: *mut IndexInfo,
) -> bool{
    warning!("inserting into the foo index");
    true
}

#[allow(non_snake_case)]
unsafe extern "C" fn foo_bulk_delete(
    _info: *mut IndexVacuumInfo,
    _stats: *mut IndexBulkDeleteResult,
    _callback: IndexBulkDeleteCallback,
    _callback_state: *mut ::core::ffi::c_void,
) -> *mut IndexBulkDeleteResult{
    warning!("called foo_bulk_delete!");
    let res = unsafe { PgBox::<pgrx::pg_sys::IndexBulkDeleteResult>::alloc() };
    res.into_pg()
}

#[allow(non_snake_case)]
unsafe extern "C" fn foo_vacuum_cleanup(
    _info: *mut IndexVacuumInfo,
    _stats: *mut IndexBulkDeleteResult,
) -> *mut IndexBulkDeleteResult{
    warning!("called foo_vacuum_cleanup!");
    let res = unsafe { PgBox::<pgrx::pg_sys::IndexBulkDeleteResult>::alloc() };
    res.into_pg()
}

#[allow(non_snake_case)]
unsafe extern "C" fn foo_cost_estimate(
    _root: *mut PlannerInfo,
    _path: *mut IndexPath,
    _loop_count: f64,
    _indexStartupCost: *mut Cost,
    _indexTotalCost: *mut Cost,
    _indexSelectivity: *mut Selectivity,
    _indexCorrelation: *mut f64,
    _indexPages: *mut f64,
){
    warning!("called foo_cost_estimate!");
}


#[allow(non_snake_case)]
unsafe extern "C" fn foo_options(
    _reloptions: Datum, 
    _validate: bool
) -> *mut bytea{
    warning!("called foo_options!");
    let res = unsafe { PgBox::<pgrx::pg_sys::bytea>::alloc() };
    res.into_pg()
}

#[allow(non_snake_case)]
unsafe extern "C" fn foo_validate(
    _opclassoid: Oid
) -> bool{
    warning!("called foo_validate!");
    true
}

#[allow(non_snake_case)]
unsafe extern "C" fn foo_begin_scan(
    indexRelation: Relation,
    nkeys: ::core::ffi::c_int,
    norderbys: ::core::ffi::c_int,
) -> IndexScanDesc{
    warning!("called foo_begin_scan!");
    let res = RelationGetIndexScan(indexRelation, nkeys, norderbys);
  
    res
}

#[allow(non_snake_case)]
unsafe extern "C" fn foo_rescan(
    
    _scan: IndexScanDesc,
    _keys: ScanKey,
    _nkeys: ::core::ffi::c_int,
    _orderbys: ScanKey,
    _norderbys: ::core::ffi::c_int,
){
    warning!("called foo_rescan!");
    // let data_len = (*scan).numberOfKeys as usize *std::mem::size_of::<ScanKeyData>();
    // std::ptr::copy((*scan).keyData, keys, data_len);
}

unsafe extern "C" fn foo_bitmap(
    _scan: IndexScanDesc, 
    tbm: *mut TIDBitmap
) -> int64{
    warning!("called foo_bitmap!");

    let tids = 0;


    let mut ptr = unsafe { PgBox::<pgrx::pg_sys::ItemPointerData>::alloc() };
    *ptr = ItemPointerData{
        ip_blkid: BlockIdData{
            bi_hi: 0 as uint16,
            bi_lo: 0 as uint16,
        },
        ip_posid: 1 as uint16
    };
    let ptr = ptr.into_pg();

    tbm_add_tuples(tbm, ptr, 1, true);


    tids.into()
}

unsafe extern "C" fn foo_end_scan(_scan: IndexScanDesc){
    warning!("called foo_begifoo_end_scann_scan!");

}


//------------------------------------------------------------------------------------------
// this was generated by the external macro but required a bit of tweaking
#[no_mangle]
#[doc(hidden)]
pub unsafe extern "C" fn foohandler_wrapper(
    fcinfo: FunctionCallInfo,
) -> Datum {
    fn _internal_wrapper<'fcx>(fcinfo: &mut FcInfo<'fcx>) -> ::pgrx::datum::Datum<'fcx> {
        #[allow(unused_unsafe)]
        unsafe {
            let call_flow = <PgBox<IndexAmRoutine,AllocatedByRust> 
                as RetAbi>::check_and_prepare(fcinfo);
            let result = match call_flow {
                CallCx::WrappedFn(mcx) => {
                    let mut mcx = PgMemoryContexts::For(mcx);
                    let _args = &mut fcinfo.args();
                    let call_result = mcx
                        .switch_to(|_| {
                            let info_ = _args
                                .next_arg_unchecked()
                                .unwrap_or_else(|| panic!("Aaaa") );
                            foohandler(info_)
                        });
                    RetAbi::to_ret(call_result)
                }
                CallCx::RestoreCx => {
                    <PgBox<IndexAmRoutine,AllocatedByRust> as RetAbi>::ret_from_fcx(fcinfo)
                }
            };
            unsafe {
                <PgBox<IndexAmRoutine,AllocatedByRust> as RetAbi>::box_ret_in(fcinfo, result)
            }
        }
    }
    let datum = unsafe {
        ::pgrx::pg_sys::submodules::panic::pgrx_extern_c_guard(|| {
            let mut fcinfo = FcInfo::from_ptr(fcinfo);
            _internal_wrapper(&mut fcinfo)
        })
    };
    datum.sans_lifetime()
}


#[no_mangle]
#[doc(hidden)]
pub extern "C" fn pg_finfo_foohandler_wrapper() -> &'static Pg_finfo_record {
    const V1_API: Pg_finfo_record = Pg_finfo_record { api_version: 1 };
    &V1_API
}

// -----------------------------------------------------------------------------------------

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn test_hello_my_extension() {
        assert_eq!("AAAAA", crate::foo());
    }

}

/// This module is required by `cargo pgrx test` invocations.
/// It must be visible at the root of your extension crate.
#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {
        // perform one-off initialization when the pg_test framework starts
    }

    #[must_use]
    pub fn postgresql_conf_options() -> Vec<&'static str> {
        // return any postgresql.conf settings that are required for your tests
        vec![]
    }
}
