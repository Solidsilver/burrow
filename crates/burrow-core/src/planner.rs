//! Pure placement planner: given blobs that need more replicas and peers with
//! free granted space, decide which peer should hold which blob. Deterministic
//! for identical inputs, so repeated ticks converge instead of thrashing.

use std::collections::BTreeMap;

/// 32-byte peer id (iroh EndpointId bytes; core stays iroh-free).
pub type PeerId = [u8; 32];

#[derive(Debug, Clone)]
pub struct BlobNeed {
    pub hash: [u8; 32],
    /// Stored (ciphertext) size of the blob.
    pub size: u64,
    /// Desired number of distinct remote holders.
    pub target: u32,
    /// Peers already holding (or fetching) this blob.
    pub holders: Vec<PeerId>,
}

#[derive(Debug, Clone)]
pub struct PeerSpace {
    pub id: PeerId,
    /// Bytes still free under the grant they gave us.
    pub free: u64,
    /// Whether the peer is currently reachable (planner only places on
    /// reachable peers; repair re-runs when liveness changes).
    pub online: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Placement {
    pub hash: [u8; 32],
    pub peer: PeerId,
}

/// Compute placements to bring blobs toward their replica targets.
///
/// Strategy: worst-deficit blobs first; each replica goes to the online peer
/// with the most remaining free space that doesn't already hold the blob.
/// Ties break on peer id, so plans are stable across runs.
pub fn plan(blobs: &[BlobNeed], peers: &[PeerSpace]) -> Vec<Placement> {
    let mut free: BTreeMap<PeerId, u64> =
        peers.iter().filter(|p| p.online).map(|p| (p.id, p.free)).collect();

    let mut ordered: Vec<&BlobNeed> = blobs
        .iter()
        .filter(|b| (b.holders.len() as u32) < b.target)
        .collect();
    ordered.sort_by(|a, b| {
        let da = a.target as i64 - a.holders.len() as i64;
        let db = b.target as i64 - b.holders.len() as i64;
        db.cmp(&da).then_with(|| a.hash.cmp(&b.hash))
    });

    let mut out = Vec::new();
    for blob in ordered {
        let mut holders: Vec<PeerId> = blob.holders.clone();
        let deficit = blob.target as usize - holders.len();
        for _ in 0..deficit {
            // Most free space first, then id, among peers not yet holding it.
            let candidate = free
                .iter()
                .filter(|(id, avail)| !holders.contains(id) && **avail >= blob.size)
                .max_by(|(id_a, a), (id_b, b)| a.cmp(b).then_with(|| id_b.cmp(id_a)))
                .map(|(id, _)| *id);
            let Some(peer) = candidate else { break }; // nowhere to put it
            *free.get_mut(&peer).unwrap() -= blob.size;
            holders.push(peer);
            out.push(Placement { hash: blob.hash, peer });
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn peer(n: u8, free: u64) -> PeerSpace {
        PeerSpace { id: [n; 32], free, online: true }
    }

    fn blob(n: u8, size: u64, target: u32, holders: &[u8]) -> BlobNeed {
        BlobNeed {
            hash: [n; 32],
            size,
            target,
            holders: holders.iter().map(|h| [*h; 32]).collect(),
        }
    }

    #[test]
    fn never_places_twice_on_same_peer() {
        let placements = plan(&[blob(1, 100, 3, &[])], &[peer(1, 1000), peer(2, 1000)]);
        assert_eq!(placements.len(), 2, "only two peers exist for target 3");
        assert_ne!(placements[0].peer, placements[1].peer);
    }

    #[test]
    fn respects_free_space() {
        let placements = plan(
            &[blob(1, 600, 1, &[]), blob(2, 600, 1, &[])],
            &[peer(1, 1000), peer(2, 500)],
        );
        // Only peer 1 can hold a 600-byte blob, and only once.
        assert_eq!(placements.len(), 1);
        assert_eq!(placements[0].peer, [1; 32]);
    }

    #[test]
    fn skips_existing_holders_and_offline() {
        let placements = plan(
            &[blob(1, 10, 2, &[1])],
            &[
                peer(1, 1000),
                PeerSpace { id: [2; 32], free: 1000, online: false },
                peer(3, 1000),
            ],
        );
        assert_eq!(placements, vec![Placement { hash: [1; 32], peer: [3; 32] }]);
    }

    #[test]
    fn deterministic_and_balanced() {
        let blobs: Vec<BlobNeed> = (0..10).map(|i| blob(i, 100, 1, &[])).collect();
        let peers = vec![peer(1, 10_000), peer(2, 10_000)];
        let a = plan(&blobs, &peers);
        let b = plan(&blobs, &peers);
        assert_eq!(a, b, "same inputs must produce the same plan");
        let on_p1 = a.iter().filter(|p| p.peer == [1; 32]).count();
        let on_p2 = a.iter().filter(|p| p.peer == [2; 32]).count();
        assert_eq!(on_p1 + on_p2, 10);
        assert!(on_p1.abs_diff(on_p2) <= 1, "placements should balance: {on_p1}/{on_p2}");
    }

    #[test]
    fn satisfied_blobs_untouched() {
        let placements = plan(&[blob(1, 10, 1, &[9])], &[peer(1, 1000)]);
        assert!(placements.is_empty());
    }
}
