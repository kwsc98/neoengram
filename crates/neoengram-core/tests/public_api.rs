use neoengram_core::{Chunk, FileNode, Tree};

#[test]
fn models_round_trip_through_json() -> Result<(), Box<dyn std::error::Error>> {
    let tree = Tree {
        files: vec![FileNode {
            path: "models/weights.pt".to_owned(),
            total_size: 4,
            chunks: vec![Chunk {
                hash: "abcd".to_owned(),
                offset: 0,
                size: 4,
            }],
        }],
    };

    let json = serde_json::to_string(&tree)?;
    let decoded: Tree = serde_json::from_str(&json)?;

    assert_eq!(decoded, tree);
    Ok(())
}
