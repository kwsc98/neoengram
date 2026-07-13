use std::{io::Cursor, num::NonZeroUsize};

use anyhow::Result;

use super::{
    contract::{ObjectCheck, PutOutcome},
    LooseObjectStore, ObjectSpec, ObjectStore,
};

#[test]
fn loose_store_satisfies_streaming_and_paging_contract() -> Result<()> {
    let temporary = tempfile::tempdir()?;
    let store = LooseObjectStore::new(temporary.path().join("objects"));
    store.initialize()?;
    store.validate_layout()?;

    let mut expected_ids = Vec::new();
    for payload in [b"charlie".as_slice(), b"alpha", b"bravo"] {
        let spec = ObjectSpec::new(
            blake3::hash(payload).to_hex().to_string(),
            u64::try_from(payload.len())?,
        )?;
        assert_eq!(
            store.put_from(&spec, &mut Cursor::new(payload))?,
            PutOutcome::Created
        );
        assert_eq!(
            store.put_from(&spec, &mut Cursor::new(payload))?,
            PutOutcome::AlreadyPresent
        );
        store.verify(&spec)?;

        let mut restored = Vec::new();
        store.copy_to(&spec, &mut restored)?;
        assert_eq!(restored, payload);
        assert_eq!(store.stat(&spec.id)?.map(|meta| meta.size), Some(spec.size));
        expected_ids.push(spec.id);
    }
    store.durability_barrier()?;

    let present = ObjectSpec::new(expected_ids[0].clone(), 7)?;
    let wrong_size = ObjectSpec::new(expected_ids[1].clone(), 999)?;
    let missing = ObjectSpec::new(blake3::hash(b"missing").to_hex().to_string(), 7)?;
    let checks = store.check_many(&[present, wrong_size.clone(), missing.clone()])?;
    assert!(matches!(checks[0], ObjectCheck::Present(_)));
    assert_eq!(
        checks[1],
        ObjectCheck::SizeMismatch {
            expected: wrong_size,
            actual_size: 5,
        }
    );
    assert_eq!(checks[2], ObjectCheck::Missing(missing));

    expected_ids.sort_unstable();
    let mut listed_ids = Vec::new();
    let mut cursor = None;
    loop {
        let page = store.list_page(cursor.as_deref(), NonZeroUsize::new(1).expect("non-zero"))?;
        listed_ids.extend(page.objects.into_iter().map(|meta| meta.id));
        let Some(next_cursor) = page.next_cursor else {
            break;
        };
        cursor = Some(next_cursor);
    }
    assert_eq!(listed_ids, expected_ids);

    let payload = b"valid prefix";
    let rejected = ObjectSpec::new(
        blake3::hash(payload).to_hex().to_string(),
        u64::try_from(payload.len())?,
    )?;
    let mut trailing = payload.to_vec();
    trailing.extend_from_slice(b" trailing");
    assert!(store
        .put_from(&rejected, &mut Cursor::new(trailing))
        .is_err());
    assert_eq!(store.stat(&rejected.id)?, None);
    Ok(())
}
