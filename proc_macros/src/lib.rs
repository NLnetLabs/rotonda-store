extern crate proc_macro;

mod maps;

use proc_macro::TokenStream;
use quote::{format_ident, quote};
use std::iter::Iterator;
use syn::parse_macro_input;

#[proc_macro_attribute]
pub fn stride_sizes(
    attr: TokenStream,
    struct_def: TokenStream,
) -> TokenStream {
    // The arguments for the macro invocation
    let attrs = parse_macro_input!(attr as syn::ExprTuple);

    let attrs = attrs.elems.iter().collect::<Vec<_>>();

    let struct_def = parse_macro_input!(struct_def as syn::ItemStruct);
    let type_name = &struct_def.ident;

    let ip_af = match attrs
        .first()
        .unwrap_or_else(|| panic!("Missing Address Family"))
    {
        syn::Expr::Path(t) => t,
        _ => panic!("Expected Adress Family Type"),
    };

    let prefix_size = match attrs
        .get(2)
        .unwrap_or_else(|| panic!("Missing Prefix Size for Address Family"))
    {
        syn::Expr::Lit(l) => l,
        l => panic!("Expected Prefix Size for Address Family, got {:?}", l),
    };

    let key_size = match attrs
        .get(3)
        .unwrap_or_else(|| panic!("Missing Key Size for Address Family"))
    {
        syn::Expr::Lit(l) => l,
        l => panic!("Expected Key Size for Address Family, got {:?}", l),
    };

    let prefixes_all_len;
    let all_len;
    let prefixes_buckets_name: syn::Ident;
    let get_root_prefix_set;

    // The name of the Struct that we're going to generate
    // We'll prepend it with the name of the TreeBitMap struct
    // that the user wants, so that our macro is a little bit
    // more hygienic, and the user can create multiple types
    // of TreeBitMap structs with different stride sizes.
    let buckets_name = if ip_af.path.is_ident("IPv4") {
        format_ident!("{}NodeBuckets4", type_name)
    } else {
        format_ident!("{}NodeBuckets6", type_name)
    };
    let store_bits = if ip_af.path.is_ident("IPv4") {
        all_len = (0..=32_u8).collect::<Vec<_>>();
        prefixes_all_len = (0..=32_u8)
            .map(|l| format_ident!("p{}", l))
            .collect::<Vec<_>>();
        prefixes_buckets_name = format_ident!("PrefixBuckets4");
        // prefix_store_bits = format_ident!("prefix_store_bits_4");
        get_root_prefix_set = quote! {
            fn get_root_prefix_set(&self, len: u8) -> &'_ PrefixSet<IPv4, M> {
                [
                    &self.p0, &self.p1, &self.p2, &self.p3, &self.p4, &self.p5, &self.p6, &self.p7, &self.p8,
                    &self.p9, &self.p10, &self.p11, &self.p12, &self.p13, &self.p14, &self.p15, &self.p16,
                    &self.p17, &self.p18, &self.p19, &self.p20, &self.p21, &self.p22, &self.p23, &self.p24,
                    &self.p25, &self.p26, &self.p27, &self.p28, &self.p29, &self.p30, &self.p31, &self.p32
                ][len as usize]
            }
        };
        crate::maps::node_buckets_map_v4()
    } else {
        all_len = (0..=128_u8).collect::<Vec<_>>();
        prefixes_all_len = (0..=128_u8)
            .map(|l| format_ident!("p{}", l))
            .collect::<Vec<_>>();

        prefixes_buckets_name = format_ident!("PrefixBuckets6");
        // prefix_store_bits = format_ident!("prefix_store_bits_6");
        get_root_prefix_set = quote! {
            fn get_root_prefix_set(&self, len: u8) -> &'_ PrefixSet<IPv6, M> {
                [
                    &self.p0, &self.p1, &self.p2, &self.p3, &self.p4, &self.p5, &self.p6, &self.p7, &self.p8,
                    &self.p9, &self.p10, &self.p11, &self.p12, &self.p13, &self.p14, &self.p15, &self.p16,
                    &self.p17, &self.p18, &self.p19, &self.p20, &self.p21, &self.p22, &self.p23, &self.p24,
                    &self.p25, &self.p26, &self.p27, &self.p28, &self.p29, &self.p30, &self.p31, &self.p32,
                    &self.p33, &self.p34, &self.p35, &self.p36, &self.p37, &self.p38, &self.p39, &self.p40,
                    &self.p41, &self.p42, &self.p43, &self.p44, &self.p45, &self.p46, &self.p47, &self.p48,
                    &self.p49, &self.p50, &self.p51, &self.p52, &self.p53, &self.p54, &self.p55, &self.p56,
                    &self.p57, &self.p58, &self.p59, &self.p60, &self.p61, &self.p62, &self.p63, &self.p64,
                    &self.p65, &self.p66, &self.p67, &self.p68, &self.p69, &self.p70, &self.p71, &self.p72,
                    &self.p73, &self.p74, &self.p75, &self.p76, &self.p77, &self.p78, &self.p79, &self.p80,
                    &self.p81, &self.p82, &self.p83, &self.p84, &self.p85, &self.p86, &self.p87, &self.p88,
                    &self.p89, &self.p90, &self.p91, &self.p92, &self.p93, &self.p94, &self.p95, &self.p96,
                    &self.p97, &self.p98, &self.p99, &self.p100, &self.p101, &self.p102, &self.p103, &self.p104,
                    &self.p105, &self.p106, &self.p107, &self.p108, &self.p109, &self.p110, &self.p111, &self.p112,
                    &self.p113, &self.p114, &self.p115, &self.p116, &self.p117, &self.p118, &self.p119, &self.p120,
                    &self.p121, &self.p122, &self.p123, &self.p124, &self.p125, &self.p126, &self.p127, &self.p128
                    ][len as usize]
            }
        };
        crate::maps::node_buckets_map_v6()
    };

    let mut strides_num: Vec<u8> = vec![];
    let mut strides = vec![];
    let mut strides_all_len = vec![];
    let mut strides_all_len_accu: Vec<u8> = vec![];
    let mut strides_all_len_level = vec![];
    let mut strides_len3 = vec![];
    let mut strides_len3_l = vec![];
    let mut strides_len4 = vec![];
    let mut strides_len4_l = vec![];
    let mut strides_len5 = vec![];
    let mut strides_len5_l = vec![];

    let mut s_accu = 0_u8;

    let attrs_s = match attrs[1] {
        syn::Expr::Array(arr) => arr,
        _ => panic!("Expected an array"),
    };
    let strides_len = attrs_s.elems.len() as u8;
    let first_stride_size = &attrs_s.elems[0];

    for (len, stride) in attrs_s.elems.iter().enumerate() {
        strides_all_len.push(format_ident!("l{}", len));

        match stride {
            syn::Expr::Lit(s) => {
                if let syn::Lit::Int(i) = &s.lit {
                    let stride_len = i.base10_digits().parse::<u8>().unwrap();
                    strides_num.push(stride_len);
                    strides_all_len_level.push(format_ident!("l{}", s_accu));

                    match stride_len {
                        3 => {
                            strides_len3.push(s_accu as usize);
                            strides_len3_l.push(format_ident!("l{}", s_accu));
                        }
                        4 => {
                            strides_len4.push(s_accu as usize);
                            strides_len4_l.push(format_ident!("l{}", s_accu));
                        }
                        5 => {
                            strides_len5.push(s_accu as usize);
                            strides_len5_l.push(format_ident!("l{}", s_accu));
                        }
                        _ => panic!("Expected a stride of 3, 4 or 5"),
                    };
                    strides_all_len_accu.push(s_accu);

                    s_accu += stride_len;
                    strides.push(format_ident!("Stride{}", stride_len))
                } else {
                    panic!("Expected an integer")
                }
            }
            _ => {
                panic!("Expected a literal")
            }
        }
    }

    // Check if the strides division makes sense
    let mut len_to_stride_arr = [0_u8; 128];
    strides_all_len_accu
        .iter()
        .zip(strides_num.iter())
        .for_each(|(acc, s)| {
            len_to_stride_arr[*acc as usize] = *s;
        });

    // These are the stride sizes as an array of u8s, padded with 0s to the
    // right. It's bounded to 42 u8s to avoid having to set a const generic
    // on the type (which would have to be carried over to its parent). So
    // if a 0 is encountered, it's the end of the strides.
    let mut stride_sizes = [0; 42];
    let (left, _right) = stride_sizes.split_at_mut(strides_len as usize);
    left.swap_with_slice(&mut strides_num);

    let struct_creation = quote! {

        #[derive(Debug)]
        pub(crate) struct #buckets_name<AF: AddressFamily> {
            // created fields for each sub-prefix (StrideNodeId) length,
            // with hard-coded field-names, like this:
            // l0: NodeSet<AF, Stride5>,
            // l5: NodeSet<AF, Stride5>,
            // l10: NodeSet<AF, Stride4>,
            // ...
            // l29: NodeSet<AF, Stride3>
            # ( #strides_all_len_level: NodeSet<#ip_af, #strides>, )*
            _af: PhantomData<AF>,
            stride_sizes: [u8; 42],
            strides_len: u8
        }

        #[derive(Debug)]
        pub(crate) struct #prefixes_buckets_name<AF: AddressFamily, M: Meta> {
            // creates a bucket for each prefix (PrefixId) length, with
            // hard-coded field-names, like this:
            // p0: PrefixSet<AF, M>,
            // p1: PrefixSet<AF, M>,
            // ...
            // p32: PrefixSet<AF, M>,
            #( #prefixes_all_len: PrefixSet<#ip_af, M>, )*
            _af: PhantomData<AF>,
            _m: PhantomData<M>,
        }

    };

    let prefix_buckets_map = if ip_af.path.is_ident("IPv4") {
        crate::maps::prefix_buckets_map_v4()
    } else {
        crate::maps::prefix_buckets_map_v6()
    };

    let prefix_buckets_impl = quote! {

        impl<AF: AddressFamily, M: Meta> PrefixBuckets<#ip_af, M> for #prefixes_buckets_name<AF, M> {
            fn init() -> #prefixes_buckets_name<AF, M> {
                #prefixes_buckets_name {
                    #( #prefixes_all_len: PrefixSet::init(1 << #prefixes_buckets_name::<AF, M>::get_bits_for_len(#all_len, 0)), )*
                    _af: PhantomData,
                    _m: PhantomData,
                }
            }

            fn remove(&mut self, id: PrefixId<#ip_af>) -> Option<M> { unimplemented!() }

            #get_root_prefix_set

            #prefix_buckets_map

        }

    };

    let struct_impl = quote! {

        impl<AF: AddressFamily> NodeBuckets<#ip_af> for #buckets_name<AF> {
            fn init() -> Self {
                #buckets_name {
                    // creates l0, l1, ... l<AF::BITS>, but only for the
                    // levels at the end of each stride, so for strides
                    // [5,5,4,3,3,3,3,3,3] is will create l0, l5, l10, l14,
                    // l17, l20, l23, l26, l29 last level will be omitted,
                    // because that will never be used (l29 has children
                    // with prefixes up to prefix-length 32 in this example).
                    #( #strides_all_len_level: NodeSet::init(#buckets_name::<AF>::len_to_store_bits(#strides_all_len_accu, 0) ), )*
                    _af: PhantomData,
                    stride_sizes: [ #( #stride_sizes, )*],
                    strides_len: #strides_len
                }
            }

            fn get_store3(&self, id: StrideNodeId<#ip_af>) -> &NodeSet<#ip_af, Stride3> {
                match id.get_id().1 as usize {
                    #( #strides_len3 => &self.#strides_len3_l, )*
                    _ => panic!(
                        "unexpected sub prefix length {} in stride size 3 ({})",
                        id.get_id().1,
                        id
                    ),
                }
            }

            fn get_store4(&self, id: StrideNodeId<#ip_af>) -> &NodeSet<#ip_af, Stride4> {
                match id.get_id().1 as usize {
                    #( #strides_len4 => &self.#strides_len4_l, )*
                    // ex.:
                    // 10 => &self.l10,
                    _ => panic!(
                        "unexpected sub prefix length {} in stride size 4 ({})",
                        id.get_id().1,
                        id
                    ),
                }
            }

            fn get_store5(&self, id: StrideNodeId<#ip_af>) -> &NodeSet<#ip_af, Stride5> {
                match id.get_id().1 as usize {
                    #( #strides_len5 => &self.#strides_len5_l, )*
                    // ex.:
                    // 0 => &self.l0,
                    // 5 => &self.l5,
                    _ => panic!(
                        "unexpected sub prefix length {} in stride size 5 ({})",
                        id.get_id().1,
                        id
                    ),
                }
            }

            #[inline]
            fn get_stride_sizes(&self) -> &[u8] {
                &self.stride_sizes[0..self.strides_len as usize]
            }

            #[inline]
            fn get_stride_for_id(&self, id: StrideNodeId<#ip_af>) -> u8 {
                [ #(#len_to_stride_arr, )* ][id.get_id().1 as usize]
            }

            #[inline]
            #store_bits

            fn get_strides_len() -> u8 {
                #strides_len
            }

            fn get_first_stride_size() -> u8 {
                #first_stride_size
            }
        }

    };

    let type_alias = quote! {
        type #type_name<M> = TreeBitMap<#ip_af, M, #buckets_name<#ip_af>, #prefixes_buckets_name<#ip_af, M>, #prefix_size, #key_size>;
    };

    let result = quote! {
        #struct_creation
        #struct_impl
        #prefix_buckets_impl
        #type_alias
    };

    TokenStream::from(result)
}

// ---------- Create Store struct -------------------------------------------

// This macro creates the struct that will be the public API for the
// PrefixStore. It aspires to be a thin wrapper around the v4 and v6 stores,
// so that users can use it AF agnostically. All methods defined in here
// should be public.

/// Creates a new, user-named struct with user-defined specified stride sizes
/// that can used as a store type. The size of the prefix and the total key in
/// the persisted storage should als be included, although these should
/// probably not be changed from their defaults.
///
/// # Usage
/// ```ignore
/// use rotonda_store::prelude::*;
/// use rotonda_store::prelude::multi::*;
/// use rotonda_store::meta_examples::PrefixAs;
///
/// const IP4_STRIDE_ARRAY: [u32; 8] = [4; 8];
/// const IP6_STRIDE_ARRAY: [u32; 32] = [4; 32];
///
/// #[create_store(((IPV4_STRIDE_ARRAY, 5, 17), (IPV6_STRIDE_ARRAY, 17, 29)))]
/// struct NuStorage;
/// ```
///
/// This will create a `NuStorage` struct, that can be used as a regular
/// store.
///
/// The stride-sizes can be any of \[3,4,5\], and they should add up to the
/// total number of bits in the address family (32 for IPv4 and 128 for IPv6).
/// Stride sizes in the array will be repeated if the sum of them falls short
/// of the total number of bits for the address family.i
///
/// The numbers 5, 17 after the first array for IPv4, and the numbers 17,
/// 29 after the second array, represent the number of bytes a prefix in the
/// key of a record in the persisted storage (disk), and the total number of
/// bytes of the key of a record in the persisted storage. An IPv4 prefix is
/// therefore 5 bytes (address part + prefix length), and 17 bytes for IPv6.
/// The total number of bytes of the key is calculated thus: prefix (5 or 17)
/// + multi_unique_id (4 bytes) + logical time of reception of the PDU into Rotonda (8 bytes).
///
/// # Example
/// ```ignore
/// use rotonda_store::prelude::*;
/// use rotonda_store::prelude::multi::*;
/// use rotonda_store::meta_examples::PrefixAs;
///
/// // The default stride sizes for IPv4, IPv6, resp.
/// #[create_store((
///     ([5, 5, 4, 3, 3, 3, 3, 3, 3, 3], 5, 17),
///     ([4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4,
///     4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4], 17, 29)
/// ))]
/// struct NuStore;
///
/// let store = Arc::new(NuStore::<PrefixAs>::new().unwrap());
/// ```
#[proc_macro_attribute]
pub fn create_store(
    attr: TokenStream,
    struct_def: TokenStream,
) -> TokenStream {
    let struct_def = parse_macro_input!(struct_def as syn::ItemStruct);
    let store_name = &struct_def.ident;

    let attr = parse_macro_input!(attr as syn::ExprTuple);
    let attrs = attr.elems.iter().collect::<Vec<_>>();

    let tuple_4 = attrs
        .first()
        .unwrap_or_else(|| panic!("No tuple ([u8], usize) for IPv4 defined"));
    let tuple_4 = match tuple_4 {
        syn::Expr::Tuple(t) => t,
        t => panic!("Expected tuple ([u8], usize), got {:?}", t),
    };

    let tuple_6 = attrs
        .get(1)
        .unwrap_or_else(|| panic!("No tuple ([u8], usize) for IPv6 defined"));
    let tuple_6 = match tuple_6 {
        syn::Expr::Tuple(t) => t,
        t => panic!("Expected tuple ([u8], usize), got {:?}", t),
    };

    let strides4 = tuple_4.elems.first().unwrap_or_else(|| {
        panic!(
            "Expected stride sizes array for IPv4, got {:?}",
            tuple_4.attrs
        )
    });
    let strides6 = tuple_6.elems.first().unwrap_or_else(|| {
        panic!(
            "Expected stride sizes array for IPv6, got {:?}",
            tuple_6.attrs
        )
    });

    let key_size4 = tuple_4.elems.get(1).unwrap_or_else(|| {
        panic!("Expected Key Size for IPv4, got {:?}", tuple_4.elems)
    });
    let key_size6 = tuple_6.elems.get(1).unwrap_or_else(|| {
        panic!("Expected Key Size for IPv6, got {:?}", tuple_6.attrs)
    });

    let prefix_size4 = tuple_4.elems.get(2).unwrap_or_else(|| {
        panic!("Expected Prefix Size for IPv4, got {:?}", tuple_4.elems)
    });
    let prefix_size6 = tuple_6.elems.get(2).unwrap_or_else(|| {
        panic!("Expected Prefix Size for IPv6, got {:?}", tuple_6.attrs)
    });

    let strides4_name = format_ident!("{}IPv4", store_name);
    let strides6_name = format_ident!("{}IPv6", store_name);

    let create_strides = quote! {
        use ::std::marker::PhantomData;
        use ::inetnum::addr::Prefix;

        #[stride_sizes((IPv4, #strides4, #key_size4, #prefix_size4))]
        struct #strides4_name;

        #[stride_sizes((IPv6, #strides6, #key_size6, #prefix_size6))]
        struct #strides6_name;
    };

    let store = quote! {
        /// A concurrently read/writable, lock-free Prefix Store, for use in a
        /// multi-threaded context.
        ///
        /// This store will hold records keyed on Prefix, and with values
        /// consisting of a multi-map (a map that can hold multiple values per
        /// key), filled with Records.
        ///
        /// Records in the store contain the metadata, a `multi_uniq_id`,
        /// logical time (to disambiguate the order of inserts into the store)
        /// and the status of the Record.
        ///
        /// Effectively this means that the store holds values for the set of
        /// `(prefix, multi_uniq_id)` pairs, where the primary key is the
        /// prefix, and the secondary key is the `multi_uniq_id`. These
        /// `multi_uniq_id`s are unique across all of the store. The store
        /// facilitates iterating over and changing the status for all
        /// prefixes per `multi_uniq_id`.
        ///
        /// The store has the concept of a global status for a
        /// `multi_uniq_id`, e.g. to set all prefixes for a `multi_uniq_id` in
        /// one atomic transaction to withdrawn. It also has local statuses
        /// per `(prefix, multi_uniq_id)` pairs, e.g. to withdraw one value
        /// for a `multi_uniq_id`.
        ///
        /// This way the store can hold RIBs for multiple peers in one
        /// data-structure.
        pub struct #store_name<
            M: Meta
        > {
            v4: #strides4_name<M>,
            v6: #strides6_name<M>,
            config: StoreConfig
        }

        impl<
                M: Meta
            > #store_name<M>
        {
            /// Creates a new empty store with a tree for IPv4 and on for IPv6.
            ///
            /// The store will be created with the default stride sizes. After
            /// creation you can wrap the store in an Arc<_> and `clone()` that
            /// for every thread that needs read access and/or write acces to
            /// it. As a convenience both read and write methods take a `&self`
            /// instead of `&mut self`.
            ///
            /// If you need custom stride sizes you can use the
            /// [`#[create_store]`](rotonda_macros::create_store) macro to
            /// create a struct with custom stride sizes.
            ///
            /// # Example
            /// ```
            /// use std::{sync::Arc, thread};
            /// use std::net::Ipv4Addr;
            ///
            /// use rotonda_store::prelude::*;
            /// use rotonda_store::prelude::multi::*;
            /// use rotonda_store::meta_examples::{NoMeta, PrefixAs};
            ///
            /// let tree_bitmap = Arc::new(MultiThreadedStore::<NoMeta>::new().unwrap());
            ///
            /// let _: Vec<_> = (0..16)
            ///      .map(|_| {
            ///         let tree_bitmap = tree_bitmap.clone();
            ///
            ///         thread::spawn(move || {
            ///              let pfxs = [
            ///                 Prefix::new_relaxed(
            ///                     Ipv4Addr::new(130, 55, 241, 0).into(),
            ///                     24,
            ///                 ),
            ///                 Prefix::new_relaxed(
            ///                     Ipv4Addr::new(130, 55, 240, 0).into(),
            ///                     24,
            ///                 )
            ///              ];
            ///
            ///              for pfx in pfxs.into_iter() {
            ///                  println!("insert {}", pfx.unwrap());
            ///                  tree_bitmap.insert(
            ///                      &pfx.unwrap(),
            ///                      Record::new(0, 0, RouteStatus::Active, NoMeta::Empty),
            ///                      None
            ///                  ).unwrap();
            ///              }
            ///          })
            ///      }).map(|t| t.join()).collect();
            /// ```
            pub fn new_with_config(
                    mut config: StoreConfig
                ) -> Result<Self, Box<dyn std::error::Error>> {

                let uuid = Uuid::new_v4();
                let mut config_v4 = config.clone();
                let mut config_v6 = config.clone();

                config_v4.persist_path = format!(
                    "{}/{}/ipv4/", config_v4.persist_path, uuid);

                config_v6.persist_path = format!(
                    "{}/{}/ipv6/", config.persist_path, uuid);

                Ok(Self {
                    v4: #strides4_name::new(config_v4)?,
                    v6: #strides6_name::new(config_v6)?,
                    config
                })
            }
        }

        impl<'a, M: Meta,
            > #store_name<M>
        {
            /// Search for and return one or more prefixes that match the
            ///given  `search_pfx` argument.   The search will return a
            ///[QueryResult] with the matching prefix,  if any, the type of
            ///match for the found prefix and the more and  less specifics for
            ///the requested prefix. The inclusion of more-  or less-specifics
            ///and the requested `match_type` is configurable  through the
            ///[MatchOptions] argument.
            ///
            /// The `match_type` in the `MatchOptions` indicates what match
            /// types can appear in the [QueryResult] result.
            ///
            /// `ExactMatch` is the most strict, and will only allow exactly
            /// matching prefixes in the result. Failing an exacly matching
            /// prefix, it will return an `EmptyMatch`.
            ///
            /// `LongestMatch` is less strict, and either an exactly matching
            /// prefix or - in case there is no exact match - a longest
            /// matching prefix will be allowed in the result. Failing both an
            /// EmptyMatch will be returned.
            ///
            /// For both `ExactMatch` and `LongestMatch` the
            /// `include_less_specifics` and `include_more_specifics`
            /// options will be respected and the result will contain the
            /// more and less specifics according to the options for the
            /// requested prefix, even if the result returns a `match_type`
            /// of `EmptyMatch`.
            ///
            /// `EmptyMatch` is the least strict, and will *always* return
            /// the requested prefix, be it exactly matching, longest matching
            /// or not matching at all (empty match), again, together with
            /// its less|more specifics (if requested). Note that the last
            /// option, the empty match in the result will never return
            /// less-specifics, but can return more-specifics for a prefix
            /// that itself is not present in the store.
            ///
            ///
            /// This table sums it up:
            ///
            /// | query match_type | possible result types                      | less-specifics? | more-specifics? |
            /// | ---------------- | ------------------------------------------ | --------------- | --------------- |
            /// | `ExactMatch`     | `ExactMatch`, `EmptyMatch`                 | maybe           | maybe           |
            /// | `LongestMatch`   | `ExactMatch`, `LongestMatch`, `EmptyMatch` | maybe           | maybe           |
            /// | `EmptyMatch`     | `ExactMatch`, `LongestMatch`, `EmptyMatch` | no for EmptyM res, maybe for others | yes for EmptyM for res, maybe for others |
            ///
            ///
            /// Note that the behavior of the CLI command `show route exact` on
            /// most router platforms can be modeled by setting the `match_type`
            /// to `ExactMatch` and `include_less_specifics` to `true`.
            ///
            /// # Example
            /// ```
            /// use std::net::Ipv4Addr;
            ///
            /// use rotonda_store::prelude::*;
            /// use rotonda_store::meta_examples::PrefixAs;
            /// use rotonda_store::prelude::multi::*;
            ///
            /// let store = MultiThreadedStore::<PrefixAs>::new().unwrap();
            /// let guard = &epoch::pin();
            ///
            /// let pfx_addr = "185.49.140.0".parse::<Ipv4Addr>()
            ///         .unwrap()
            ///         .into();
            ///
            /// store.insert(
            ///     &Prefix::new(pfx_addr, 22).unwrap(),
            ///     Record::new(0, 0, RouteStatus::Active, PrefixAs::new_from_u32(211321)),
            ///     None
            /// );
            ///
            /// let res = store.match_prefix(
            ///     &Prefix::new(pfx_addr, 24).unwrap(),
            ///     &MatchOptions {
            ///         match_type: MatchType::LongestMatch,
            ///         include_withdrawn: false,
            ///         include_less_specifics: false,
            ///         include_more_specifics: false,
            ///         mui: None
            ///     },
            ///     guard
            /// );
            ///
            /// assert_eq!(res.prefix_meta[0].meta.asn(), 211321.into());
            ///
            /// let res = store.match_prefix(
            ///     &Prefix::new(pfx_addr, 24).unwrap(),
            ///         &MatchOptions {
            ///             match_type: MatchType::ExactMatch,
            ///             include_withdrawn: false,
            ///             include_less_specifics: false,
            ///             include_more_specifics: false,
            ///             mui: None
            ///         },
            ///         guard
            ///     );
            ///
            /// assert!(res.match_type.is_empty());
            ///
            /// ```
            pub fn match_prefix(
                &'a self,
                search_pfx: &Prefix,
                options: &MatchOptions,
                guard: &'a Guard,
            ) -> QueryResult<M> {

                match search_pfx.addr() {
                    std::net::IpAddr::V4(addr) => {
                        self.v4.match_prefix(
                            PrefixId::<IPv4>::new(
                                addr.into(),
                                search_pfx.len(),
                            ),
                            options,
                            guard
                        )
                    },
                    std::net::IpAddr::V6(addr) => {
                        self.v6.match_prefix(
                            PrefixId::<IPv6>::new(
                                addr.into(),
                                search_pfx.len(),
                            ),
                            options,
                            guard
                        )
                    }
                }
            }

            /// Return the record that belongs to the pre-calculated and
            /// stored best path for a given prefix.
            ///
            /// If the Prefix does not exist in the store `None` is returned.
            /// If the prefix does exist, but no best path was calculated
            /// (yet), a `PrefixStoreError::BestPathNotFound` error will be
            /// returned. A returned result of
            /// `PrefixError::StoreNotReadyError` should never happen: it
            /// would indicate an internal inconsistency in the store.
            pub fn best_path(&'a self,
                search_pfx: &Prefix,
                guard: &Guard
            ) -> Option<Result<Record<M>, PrefixStoreError>> {
                match search_pfx.addr() {
                    std::net::IpAddr::V4(addr) => self.v4.best_path(
                        PrefixId::<IPv4>::new(
                            addr.into(),
                            search_pfx.len()
                        ),
                        guard
                    ),
                    std::net::IpAddr::V6(addr) => self.v6.best_path(
                        PrefixId::<IPv6>::new(
                            addr.into(),
                            search_pfx.len(),
                        ),
                        guard
                    )
                }
            }

            /// Calculate and store the best path for the specified Prefix.
            ///
            /// If the result of the calculation is successful it will be
            /// stored for the prefix. If they were set, it will return the
            /// multi_uniq_id of the best path and the one for the backup
            /// path, respectively. If the prefix does not exist in the store,
            /// `None` will be returned. If the best path cannot be
            /// calculated, a `Ok(None, None)` will be returned.
            ///
            /// Failing to calculate a best path, may be caused by
            /// unavailability of any active paths, or by a lack of data (in
            /// either the paths, or the supplied `TiebreakerInfo`).
            ///
            /// An Error result indicates an inconsistency in the store.
            pub fn calculate_and_store_best_and_backup_path(
                &self,
                search_pfx: &Prefix,
                tbi: &<M as Meta>::TBI,
                guard: &Guard
            ) -> Result<(Option<u32>, Option<u32>), PrefixStoreError> {
                match search_pfx.addr() {
                    std::net::IpAddr::V4(addr) => self.v4
                        .calculate_and_store_best_and_backup_path(
                            PrefixId::<IPv4>::new(
                                addr.into(),
                                search_pfx.len(),
                            ),
                            tbi,
                            guard
                        ),
                    std::net::IpAddr::V6(addr) => self.v6
                        .calculate_and_store_best_and_backup_path(
                            PrefixId::<IPv6>::new(
                                addr.into(),
                                search_pfx.len(),
                            ),
                            tbi,
                            guard
                        )
                }
            }

            /// Return whether the best path selection, stored for this prefix
            /// in the store is up to date with all the routes stored for
            /// this prefix.
            ///
            /// Will return an error if the prefix does not exist in the store
            ///
            /// The `guard` should be a `&epoch::pin()`. It allows the
            /// QuerySet to contain references to the meta-data objects,
            /// instead of cloning them into it.
            pub fn is_ps_outdated(
                &self,
                search_pfx: &Prefix,
                guard: &Guard
            ) -> Result<bool, PrefixStoreError> {
                match search_pfx.addr() {
                    std::net::IpAddr::V4(addr) => self.v4
                        .is_ps_outdated(
                            PrefixId::<IPv4>::new(
                                addr.into(),
                                search_pfx.len(),
                            ),
                            guard
                        ),
                    std::net::IpAddr::V6(addr) => self.v6
                        .is_ps_outdated(
                            PrefixId::<IPv6>::new(
                                addr.into(),
                                search_pfx.len(),
                            ),
                            guard
                        )
                }
            }

            /// Return a [QueryResult] that contains all the more-specific
            /// prefixes of the `search_pfx` in the store, including the
            /// meta-data of these prefixes.
            ///
            /// The `search_pfx` argument can be either a IPv4 or an IPv6
            /// prefix. The `search_pfx` itself doesn't have to be present
            /// in the store for an iterator to be non-empty, i.e. if
            /// more-specific prefixes exist for a non-existent
            /// `search_pfx` the iterator will yield these more-specific
            /// prefixes.
            ///
            /// The `guard` should be a `&epoch::pin()`. It allows the
            /// QuerySet to contain references to the meta-data objects,
            /// instead of cloning them into it.
            pub fn more_specifics_from(&'a self,
                search_pfx: &Prefix,
                mui: Option<u32>,
                include_withdrawn: bool,
                guard: &'a Guard,
            ) -> QueryResult<M> {

                match search_pfx.addr() {
                    std::net::IpAddr::V4(addr) => self.v4.more_specifics_from(
                        PrefixId::<IPv4>::new(
                            addr.into(),
                            search_pfx.len(),
                        ),
                        mui,
                        include_withdrawn,
                        guard
                    ),
                    std::net::IpAddr::V6(addr) => self.v6.more_specifics_from(
                        PrefixId::<IPv6>::new(
                            addr.into(),
                            search_pfx.len(),
                        ),
                        mui,
                        include_withdrawn,
                        guard
                    ),
                }
            }

            /// Return a `QuerySet` that contains all the less-specific
            /// prefixes of the `search_pfx` in the store, including the
            /// meta-data of these prefixes.
            ///
            /// The `search_pfx` argument can be either a IPv4 or an IPv6
            /// prefix. The `search_pfx` itself doesn't have to be present
            /// in the store for an iterator to be non-empty, i.e. if
            /// less-specific prefixes exist for a non-existent
            /// `search_pfx` the iterator will yield these less-specific
            /// prefixes.
            ///
            /// The `guard` should be a `&epoch::pin()`. It allows the
            /// QuerySet to contain references to the meta-data objects,
            /// instead of cloning them into it.
            pub fn less_specifics_from(&'a self,
                search_pfx: &Prefix,
                mui: Option<u32>,
                include_withdrawn: bool,
                guard: &'a Guard,
            ) -> QueryResult<M> {

                match search_pfx.addr() {
                    std::net::IpAddr::V4(addr) => self.v4.less_specifics_from(
                        PrefixId::<IPv4>::new(
                            addr.into(),
                            search_pfx.len(),
                        ),
                        mui,
                        include_withdrawn,
                        guard
                    ),
                    std::net::IpAddr::V6(addr) => self.v6.less_specifics_from(
                        PrefixId::<IPv6>::new(
                            addr.into(),
                            search_pfx.len(),
                        ),
                        mui,
                        include_withdrawn,
                        guard
                    ),
                }
            }

            /// Returns an iterator over all the less-specific prefixes
            /// of the `search_prefix`, if present in the store, including
            /// the meta-data of these prefixes.
            ///
            /// The `search_pfx` argument can be either a IPv4 or an IPv6
            /// prefix. The `search_pfx` itself doesn't have to be present
            /// in the store for an iterator to be non-empty, i.e. if
            /// less-specific prefixes exist for a non-existent
            /// `search_pfx` the iterator will yield these less-specific
            /// prefixes.
            ///
            /// The `guard` should be a `&epoch::pin()`. It allows the
            /// iterator to create and return references to the meta-data
            /// objects to the caller (instead of cloning them).
            ///
            /// # Example
            /// ```
            /// use std::net::Ipv4Addr;
            ///
            /// use rotonda_store::prelude::*;
            /// use rotonda_store::meta_examples::PrefixAs;
            /// use rotonda_store::prelude::multi::*;
            ///
            ///
            /// let store = MultiThreadedStore::<PrefixAs>::new().unwrap();
            /// let guard = epoch::pin();
            ///
            /// let pfx_addr = "185.49.140.0".parse::<Ipv4Addr>()
            ///         .unwrap()
            ///         .into();
            ///
            /// store.insert(
            ///     &Prefix::new(pfx_addr, 22).unwrap(),
            ///     Record::new(0, 0, RouteStatus::Active, PrefixAs::new_from_u32(211321)),
            ///     None
            /// );
            ///
            /// for prefix_record in store.less_specifics_iter_from(
            ///     &Prefix::new(pfx_addr, 24).unwrap(),
            ///     None,
            ///     false,
            ///     &guard
            /// ) {
            ///    assert_eq!(prefix_record.meta[0].meta.asn(), 211321.into());
            /// }
            /// ```
            pub fn less_specifics_iter_from(&'a self,
                search_pfx: &Prefix,
                mui: Option<u32>,
                include_withdrawn: bool,
                guard: &'a Guard,
                ) -> impl Iterator<Item=PrefixRecord<M>> + 'a {
                    let (left, right) = match search_pfx.addr() {
                        std::net::IpAddr::V4(addr) => {
                            (
                                Some(self.v4.store.less_specific_prefix_iter(
                                        PrefixId::<IPv4>::new(
                                            addr.into(),
                                            search_pfx.len(),
                                        ),
                                        mui,
                                        include_withdrawn,
                                        guard
                                    )
                                    .map(|p| PrefixRecord::from(p))
                                ),
                                None
                            )
                        }
                        std::net::IpAddr::V6(addr) => {
                            (
                                None,
                                Some(self.v6.store.less_specific_prefix_iter(
                                        PrefixId::<IPv6>::new(
                                            addr.into(),
                                            search_pfx.len(),
                                        ),
                                        mui,
                                        include_withdrawn,
                                        guard
                                    )
                                    .map(|p| PrefixRecord::from(p))
                                )
                            )
                        }
                    };
                    left.into_iter().flatten().chain(right.into_iter().flatten())
                }

            /// Returns an iterator over all the more-specifics prefixes
            /// of the `search_prefix`, if present in the store, including
            /// the meta-data of these prefixes.
            ///
            /// The `search_pfx` argument can be either a IPv4 or an IPv6
            /// prefix. The `search_pfx` itself doesn't have to be present
            /// in the store for an iterator to be non-empty, i.e. if
            /// more-specific prefixes exist for a non-existent
            /// `search_pfx` the iterator will yield these more-specific
            /// prefixes.
            ///
            /// The `guard` should be a `&epoch::pin()`. It allows the
            /// iterator to create and return references to the meta-data
            /// objects to the caller (instead of cloning them).
            ///
            /// # Example
            /// ```
            /// use std::net::Ipv4Addr;
            ///
            /// use rotonda_store::prelude::*;
            /// use rotonda_store::prelude::multi::*;
            /// use rotonda_store::meta_examples::PrefixAs;
            ///
            /// let store = MultiThreadedStore::<PrefixAs>::new().unwrap();
            /// let guard = epoch::pin();
            ///
            /// let pfx_addr = "185.49.140.0".parse::<Ipv4Addr>()
            ///         .unwrap()
            ///         .into();
            ///
            /// store.insert(
            ///     &Prefix::new(pfx_addr, 24).unwrap(),
            ///     Record::new(0, 0, RouteStatus::Active, PrefixAs::new_from_u32(211321)),
            ///     None
            /// );
            ///
            /// for prefix_record in store.more_specifics_iter_from(
            ///     &Prefix::new(pfx_addr, 22).unwrap(),
            ///     None,
            ///     false,
            ///     &guard
            /// ) {
            ///    assert_eq!(prefix_record.meta[0].meta.asn(), 211321.into());
            /// }
            /// ```
            pub fn more_specifics_iter_from(&'a self,
                search_pfx: &Prefix,
                mui: Option<u32>,
                include_withdrawn: bool,
                guard: &'a Guard,
            ) -> impl Iterator<Item=PrefixRecord<M>> + 'a {

                let (left, right) = match search_pfx.addr() {
                    std::net::IpAddr::V4(addr) => {
                        let bmin = unsafe {
                            self.v4.store.withdrawn_muis_bmin.load(
                                Ordering::Acquire, guard
                            ).deref()
                        };
                        if mui.is_some() && bmin.contains(mui.unwrap()) {
                                (None, None)
                            } else {
                                (
                                    Some(self.v4.store.more_specific_prefix_iter_from(
                                            PrefixId::<IPv4>::new(
                                                addr.into(),
                                                search_pfx.len(),
                                            ),
                                            mui,
                                            include_withdrawn,
                                            guard
                                        ).map(|p| PrefixRecord::from(p))
                                    ),
                                    None
                                )
                            }
                        }
                    std::net::IpAddr::V6(addr) => {
                        let bmin = unsafe {
                            self.v6.store.withdrawn_muis_bmin.load(
                                Ordering::Acquire, guard
                            ).deref()
                        };
                        if mui.is_some() && bmin.contains(mui.unwrap()) {
                            (None, None)
                        } else {
                            (
                                None,
                                Some(self.v6.store.more_specific_prefix_iter_from(
                                        PrefixId::<IPv6>::new(
                                            addr.into(),
                                            search_pfx.len(),
                                        ),
                                        mui,
                                        include_withdrawn,
                                        guard
                                    ).map(|p| PrefixRecord::from(p))
                                )
                            )
                        }
                    }
                };
                left.into_iter().flatten().chain(right.into_iter().flatten())
            }

            pub fn iter_records_for_mui_v4(
                &'a self,
                mui: u32,
                include_withdrawn: bool,
                guard: &'a Guard
            ) -> impl Iterator<Item=PrefixRecord<M>> +'a {

                let bmin = unsafe {
                    self.v4.store.withdrawn_muis_bmin.load(
                        Ordering::Acquire, guard
                    ).deref()
                };

                if bmin.contains(mui) && !include_withdrawn {
                    None
                } else {
                    Some(
                        self.v4.store.more_specific_prefix_iter_from(
                                PrefixId::<IPv4>::new(
                                    0,
                                    0,
                                ),
                                Some(mui),
                                include_withdrawn,
                                guard
                            ).map(|p| PrefixRecord::from(p))
                    )
                }.into_iter().flatten()
            }

            pub fn iter_records_for_mui_v6(
                &'a self,
                mui: u32,
                include_withdrawn: bool,
                guard: &'a Guard
            ) -> impl Iterator<Item=PrefixRecord<M>> +'a {

                let bmin = unsafe {
                    self.v4.store.withdrawn_muis_bmin.load(
                        Ordering::Acquire, guard
                    ).deref()
                };

                if bmin.contains(mui) && !include_withdrawn {
                    None
                } else {
                    Some(
                        self.v6.store.more_specific_prefix_iter_from(
                                PrefixId::<IPv6>::new(
                                    0,
                                    0,
                                ),
                                Some(mui),
                                include_withdrawn,
                                guard
                            ).map(|p| PrefixRecord::from(p))
                    )
                }.into_iter().flatten()
            }

            /// Insert or replace a Record into the Store
            ///
            /// The specified Record will replace an existing record in the
            /// store if the multi-map for the specified prefix already has an
            /// entry for the `multi_uniq_id`, otherwise it will be added to
            /// the multi-map.
            ///
            /// If the `update_path_sections` argument is used the best path
            /// selection will be run on the resulting multi-map after insert
            /// and stored for the specified prefix.
            ///
            /// Returns some metrics about the resulting insert.
            pub fn insert(
                &self,
                prefix: &Prefix,
                record: Record<M>,
                update_path_selections: Option<M::TBI>
            ) -> Result<UpsertReport, PrefixStoreError> {
                match prefix.addr() {
                    std::net::IpAddr::V4(addr) => {
                        self.v4.insert(
                            PrefixId::<IPv4>::from(*prefix),
                            record,
                            update_path_selections,
                        )
                    }
                    std::net::IpAddr::V6(addr) => {
                        self.v6.insert(
                            PrefixId::<IPv6>::from(*prefix),
                            record,
                            update_path_selections,
                        )
                    }
                }
            }

            /// Returns an unordered iterator over all prefixes, with any
            /// status (including Withdrawn), for both IPv4 and IPv6,
            /// currently in the store, including meta-data.
            ///
            /// Although the iterator is unordered within an address-family,
            /// it first iterates over all IPv4 addresses and then over all
            /// IPv6 addresses.
            ///
            /// The `guard` should be a `&epoch::pin()`. It allows the
            /// iterator to create and return references to the meta-data
            /// objects to the caller (instead of cloning them).
            ///
            /// # Example
            /// ```
            /// use std::net::Ipv4Addr;
            ///
            /// use rotonda_store::prelude::*;
            /// use rotonda_store::prelude::multi::*;
            /// use rotonda_store::meta_examples::PrefixAs;
            ///
            /// let store = MultiThreadedStore::<PrefixAs>::new().unwrap();
            /// let guard = epoch::pin();
            ///
            /// let pfx_addr = "185.49.140.0".parse::<Ipv4Addr>()
            ///         .unwrap()
            ///         .into();
            /// let our_asn = Record::new(0, 0, RouteStatus::Active, PrefixAs::new_from_u32(211321));
            ///
            /// store.insert(&Prefix::new(pfx_addr, 22).unwrap(), our_asn.clone(), None);
            /// store.insert(&Prefix::new(pfx_addr, 23).unwrap(), our_asn.clone(), None);
            /// store.insert(&Prefix::new(pfx_addr, 24).unwrap(), our_asn.clone(), None);
            /// store.insert(&Prefix::new(pfx_addr, 25).unwrap(), our_asn, None);
            ///
            /// let mut iter = store.prefixes_iter();
            ///
            /// assert_eq!(iter.next().unwrap().prefix,
            ///     Prefix::new(pfx_addr, 22).unwrap());
            /// assert_eq!(iter.next().unwrap().prefix,
            ///     Prefix::new(pfx_addr, 23).unwrap());
            /// assert_eq!(iter.next().unwrap().prefix,
            ///     Prefix::new(pfx_addr, 24).unwrap());
            /// assert_eq!(iter.next().unwrap().prefix,
            ///     Prefix::new(pfx_addr, 25).unwrap());
            /// ```
            pub fn prefixes_iter(
                &'a self,
            ) -> impl Iterator<Item=PrefixRecord<M>> + 'a {
                self.v4.store.prefixes_iter()
                    .map(|p| PrefixRecord::from(p))
                    .chain(
                        self.v6.store.prefixes_iter()
                        .map(|p| PrefixRecord::from(p))
                    )
            }

            /// Returns an unordered iterator over all IPv4 prefixes in the
            /// currently in the store, with any status (including Withdrawn),
            /// including meta-data.
            ///
            /// The `guard` should be a `&epoch::pin()`. It allows the
            /// iterator to create and return references to the meta-data
            /// objects to the caller (instead of cloning them).
            ///
            /// # Example
            /// ```
            /// use std::net::Ipv4Addr;
            ///
            /// use rotonda_store::prelude::*;
            /// use rotonda_store::prelude::multi::*;
            /// use rotonda_store::meta_examples::PrefixAs;
            ///
            /// let store = MultiThreadedStore::<PrefixAs>::new().unwrap();
            /// let guard = epoch::pin();
            ///
            /// let pfx_addr = "185.49.140.0".parse::<Ipv4Addr>()
            ///         .unwrap()
            ///         .into();
            /// let our_asn = Record::new(0, 0, RouteStatus::Active, PrefixAs::new_from_u32(211321));
            ///
            /// store.insert(&Prefix::new(pfx_addr, 22).unwrap(), our_asn.clone(), None);
            /// store.insert(&Prefix::new(pfx_addr, 23).unwrap(), our_asn.clone(), None);
            /// store.insert(&Prefix::new(pfx_addr, 24).unwrap(), our_asn.clone(), None);
            /// store.insert(&Prefix::new(pfx_addr, 25).unwrap(), our_asn, None);
            ///
            /// let mut iter = store.prefixes_iter();
            ///
            /// assert_eq!(iter.next().unwrap().prefix,
            ///     Prefix::new(pfx_addr, 22).unwrap());
            /// assert_eq!(iter.next().unwrap().prefix,
            ///     Prefix::new(pfx_addr, 23).unwrap());
            /// assert_eq!(iter.next().unwrap().prefix,
            ///     Prefix::new(pfx_addr, 24).unwrap());
            /// assert_eq!(iter.next().unwrap().prefix,
            ///     Prefix::new(pfx_addr, 25).unwrap());
            /// ```
            pub fn prefixes_iter_v4(
                &'a self,
            ) -> impl Iterator<Item=PrefixRecord<M>> + 'a {
                self.v4.store.prefixes_iter()
                    .map(|p| PrefixRecord::from(p))
            }

            /// Returns an unordered iterator over all IPv6 prefixes in the
            /// currently in the store, with any status (including Withdrawn),
            /// including meta-data.
            ///
            /// The `guard` should be a `&epoch::pin()`. It allows the
            /// iterator to create and return references to the meta-data
            /// objects to the caller (instead of cloning them).
            ///
            /// # Example
            /// ```
            /// use std::net::Ipv6Addr;
            ///
            /// use rotonda_store::prelude::*;
            /// use rotonda_store::prelude::multi::*;
            /// use rotonda_store::meta_examples::PrefixAs;
            ///
            /// let store = MultiThreadedStore::<PrefixAs>::new().unwrap();
            /// let guard = epoch::pin();
            ///
            /// let pfx_addr = "2a04:b900::".parse::<Ipv6Addr>()
            ///         .unwrap()
            ///         .into();
            /// let our_asn = Record::new(0, 0, RouteStatus::Active, PrefixAs::new_from_u32(211321));
            ///
            /// store.insert(&Prefix::new(pfx_addr, 29).unwrap(), our_asn.clone(), None);
            /// store.insert(&Prefix::new(pfx_addr, 48).unwrap(), our_asn.clone(), None);
            /// store.insert(&Prefix::new(pfx_addr, 56).unwrap(), our_asn.clone(), None);
            /// store.insert(&Prefix::new(pfx_addr, 64).unwrap(), our_asn, None);
            ///
            /// let mut iter = store.prefixes_iter();
            ///
            /// assert_eq!(iter.next().unwrap().prefix,
            ///     Prefix::new(pfx_addr, 29).unwrap());
            /// assert_eq!(iter.next().unwrap().prefix,
            ///     Prefix::new(pfx_addr, 48).unwrap());
            /// assert_eq!(iter.next().unwrap().prefix,
            ///     Prefix::new(pfx_addr, 56).unwrap());
            /// assert_eq!(iter.next().unwrap().prefix,
            ///     Prefix::new(pfx_addr, 64).unwrap());
            /// ```
            pub fn prefixes_iter_v6(
                &'a self,
            ) -> impl Iterator<Item=PrefixRecord<M>> + 'a {
                self.v6.store.prefixes_iter()
                    .map(|p| PrefixRecord::from(p))
            }

            /// Change the local status of the record for the combination of
            /// (prefix, multi_uniq_id) to Withdrawn. Note that by default the
            /// global `Withdrawn` status for a mui overrides the local status
            /// of a record.
            pub fn mark_mui_as_withdrawn_for_prefix(
                &self,
                prefix: &Prefix,
                mui: u32
            ) -> Result<(), PrefixStoreError> {
                let guard = &epoch::pin();
                match prefix.addr() {
                    std::net::IpAddr::V4(addr) => {
                        self.v4.store.mark_mui_as_withdrawn_for_prefix(
                            PrefixId::<IPv4>::from(*prefix),
                            mui,
                            // &guard
                        )
                    }
                    std::net::IpAddr::V6(addr) => {
                        self.v6.store.mark_mui_as_withdrawn_for_prefix(
                            PrefixId::<IPv6>::from(*prefix),
                            mui,
                            // &guard
                        )
                    }
                }
            }

            /// Change the local status of the record for the combination of
            /// (prefix, multi_uniq_id) to Active. Note that by default the
            /// global `Withdrawn` status for a mui overrides the local status
            /// of a record.
            pub fn mark_mui_as_active_for_prefix(
                &self,
                prefix: &Prefix,
                mui: u32
            ) -> Result<(), PrefixStoreError> {
                let guard = &epoch::pin();
                match prefix.addr() {
                    std::net::IpAddr::V4(addr) => {
                        self.v4.store.mark_mui_as_active_for_prefix(
                            PrefixId::<IPv4>::from(*prefix),
                            mui,
                        )
                    }
                    std::net::IpAddr::V6(addr) => {
                        self.v6.store.mark_mui_as_active_for_prefix(
                            PrefixId::<IPv6>::from(*prefix),
                            mui,
                        )
                    }
                }
            }

            /// Change the status of all records for IPv4 prefixes for this
            /// `multi_uniq_id` globally to Active.  Note that the global
            /// `Active` status will be overridden by the local status of the
            /// record.
            pub fn mark_mui_as_active_v4(
                &self,
                mui: u32
            ) -> Result<(), PrefixStoreError> {
                let guard = &epoch::pin();

                self.v4.store.mark_mui_as_active(
                    mui,
                    &guard
                )
            }

            /// Change the status of all records for IPv4 prefixes for this
            /// `multi_uniq_id` globally to Withdrawn. A global `Withdrawn`
            /// status for a `multi_uniq_id` overrides the local status of
            /// prefixes for this mui. However the local status can still be
            /// modified. This modification will take effect if the global
            /// status is changed to `Active`.
            pub fn mark_mui_as_withdrawn_v4(
                &self,
                mui: u32
            ) -> Result<(), PrefixStoreError> {
                let guard = &epoch::pin();

                self.v4.store.mark_mui_as_withdrawn(
                    mui,
                    &guard
                )
            }

            /// Change the status of all records for IPv6 prefixes for this
            /// `multi_uniq_id` globally to Active.  Note that the global
            /// `Active` status will be overridden by the local status of the
            /// record.
            pub fn mark_mui_as_active_v6(
                &self,
                mui: u32
            ) -> Result<(), PrefixStoreError> {
                let guard = &epoch::pin();

                self.v6.store.mark_mui_as_active(
                    mui,
                    &guard
                )
            }

            /// Change the status of all records for IPv6 prefixes for this
            /// `multi_uniq_id` globally to Withdrawn. A global `Withdrawn`
            /// status for a `multi_uniq_id` overrides the local status of
            /// prefixes for this mui. However the local status can still be
            /// modified. This modification will take effect if the global
            /// status is changed to `Active`.
            pub fn mark_mui_as_withdrawn_v6(
                &self,
                mui: u32
            ) -> Result<(), PrefixStoreError> {
                let guard = &epoch::pin();

                self.v6.store.mark_mui_as_withdrawn(
                    mui,
                    &guard
                )
            }


            /// Change the status of all records for this `multi_uniq_id` to
            /// Withdrawn.
            ///
            /// This method tries to mark all records: first the IPv4 records,
            /// then the IPv6 records. If marking of the IPv4 records fails,
            /// the method continues and tries to mark the IPv6 records. If
            /// either or both fail, an error is returned.
            pub fn mark_mui_as_withdrawn(
                &self,
                mui: u32
            ) -> Result<(), PrefixStoreError> {
                let guard = &epoch::pin();

                let res_v4 = self.v4.store.mark_mui_as_withdrawn(
                    mui,
                    &guard
                );
                let res_v6 = self.v6.store.mark_mui_as_withdrawn(
                    mui,
                    &guard
                );

                res_v4.and(res_v6)
            }



            // Whether the global status for IPv4 prefixes and the specified
            // `multi_uniq_id` is set to `Withdrawn`.
            pub fn mui_is_withdrawn_v4(
                &self,
                mui: u32
            ) -> bool {
                let guard = &epoch::pin();

                self.v4.store.mui_is_withdrawn(mui, guard)
            }

            // Whether the global status for IPv6 prefixes and the specified
            // `multi_uniq_id` is set to `Active`.
            pub fn mui_is_withdrawn_v6(
                &self,
                mui: u32
            ) -> bool {
                let guard = &epoch::pin();

                self.v6.store.mui_is_withdrawn(mui, guard)
            }

            /// Returns the number of all prefixes in the store.
            ///
            /// Note that this method will actually traverse the complete
            /// tree.
            pub fn prefixes_count(&self) -> usize {
                self.v4.store.get_prefixes_count()
                + self.v6.store.get_prefixes_count()
            }

            /// Returns the number of all IPv4 prefixes in the store.
            ///
            /// Note that this counter may be lower than the actual
            /// number in the store, due to contention at the time of
            /// reading the value.
            pub fn prefixes_v4_count(&self) -> usize {
                self.v4.store.get_prefixes_count()
            }

            /// Returns the number of all IPv4 prefixes with the
            /// supplied prefix length in the store.
            ///
            /// Note that this counter may be lower than the actual
            /// number in the store, due to contention at the time of
            /// reading the value.
            pub fn prefixes_v4_count_for_len(&self, len: u8) -> usize {
                self.v4.store.get_prefixes_count_for_len(len)
            }

            /// Returns the number of all IPv6 prefixes in the store.
            ///
            /// Note that this counter may be lower than the actual
            /// number in the store, due to contention at the time of
            /// reading the value.
            pub fn prefixes_v6_count(&self) -> usize {
                self.v6.store.get_prefixes_count()
            }

            /// Returns the number of all IPv6 prefixes with the
            /// supplied prefix length in the store.
            ///
            /// Note that this counter may be lower than the actual
            /// number in the store, due to contention at the time of
            /// reading the value.
            pub fn prefixes_v6_count_for_len(&self, len: u8) -> usize {
                self.v6.store.get_prefixes_count_for_len(len)
            }

            /// Returns the number of nodes in the store.
            ///
            /// Note that this counter may be lower than the actual
            /// number in the store, due to contention at the time of
            /// reading the value.
            pub fn nodes_count(&self) -> usize {
                self.v4.store.get_nodes_count()
                + self.v6.store.get_nodes_count()
            }

            /// Returns the number of IPv4 nodes in the store.
            ///
            /// Note that this counter may be lower than the actual
            /// number in the store, due to contention at the time of
            /// reading the value.
            pub fn nodes_v4_count(&self) -> usize {
                self.v4.store.get_nodes_count()
            }

            /// Returns the number of IPv6 nodes in the store.
            ///
            /// Note that this counter may be lower than the actual
            /// number in the store, due to contention at the time of
            /// reading the value.
            pub fn nodes_v6_count(&self) -> usize {
                self.v6.store.get_nodes_count()
            }

            /// Print the store statistics to the standard output.
            #[cfg(feature = "cli")]
            pub fn print_funky_stats(&self) {
                println!("");
                println!("Stats for IPv4 multi-threaded store\n");
                println!("{}", self.v4);
                println!("Stats for IPv6 multi-threaded store\n");
                println!("{}", self.v6);
            }

            // The Store statistics.
            pub fn stats(&self) -> StoreStats {
                StoreStats {
                    v4: self.v4.store.counters.get_prefix_stats(),
                    v6: self.v6.store.counters.get_prefix_stats(),
                }
            }

            // Disk Persistence

            pub fn persist_strategy(&self) -> PersistStrategy {
                self.v4.store.config.persist_strategy()
            }

            pub fn get_records_for_prefix(&self, prefix: &Prefix) ->
                Vec<Record<M>> {
                match prefix.is_v4() {
                    true => self.v4.store.persistence.as_ref().map_or(vec![],
                        |p| p.get_records_for_prefix(
                            PrefixId::<IPv4>::from(*prefix)
                        )),
                    false => self.v6.store.persistence.as_ref().map_or(vec![],
                        |p| p.get_records_for_prefix(
                            PrefixId::<IPv6>::from(*prefix)
                        ))
                }
            }

            /// Persist all the non-unique (prefix, mui, ltime) tuples
            /// with their values to disk
            pub fn flush_to_disk(&self) -> Result<(), lsm_tree::Error> {
                if let Some(persistence) =
                    self.v4.store.persistence.as_ref() {
                        persistence.flush_to_disk()?;
                }

                if let Some(persistence) =
                    self.v6.store.persistence.as_ref() {
                        persistence.flush_to_disk()?;
                }

                Ok(())
            }

            /// Return the approximate number of items that are persisted
            /// to disk, for IPv4 and IPv6 respectively.
            pub fn approx_persisted_items(&self) -> (usize, usize) {
                (
                    self.v4.store.persistence.as_ref().map_or(0, |p| p.approximate_len()),
                    self.v6.store.persistence.as_ref().map_or(0, |p| p.approximate_len())
                )
            }

            /// Return an estimation of the disk space currently used by the
            /// store in bytes.
            pub fn disk_space(&self) -> u64 {
                self.v4.store.persistence.as_ref().map_or(0, |p| p.disk_space()) +
                self.v6.store.persistence.as_ref().map_or(0, |p| p.disk_space())
            }
        }
    };

    let result = quote! {
        #create_strides
        #store
    };

    TokenStream::from(result)
}
