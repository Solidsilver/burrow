//! Pure placement planner: given blobs that need more replicas and devices
//! with free granted space, decide which device should hold which blob.
//! Deterministic for identical inputs, so repeated ticks converge.
//!
//! Devices are the unit of placement; owners (people) are the unit of
//! diversity: replicas prefer distinct owners, and `min_offsite` guarantees
//! copies on owners other than yourself.

use std::collections::BTreeMap;

/// 32-byte ids (iroh EndpointId / owner public key bytes; core stays iroh-free).
pub type DeviceId = [u8; 32];
pub type OwnerId = [u8; 32];

#[derive(Debug, Clone)]
pub struct BlobNeed {
    pub hash: [u8; 32],
    /// Stored (ciphertext) size of the blob.
    pub size: u64,
    /// Desired number of distinct devices holding the blob.
    pub target: u32,
    /// Required number of holders belonging to owners OTHER than self.
    pub min_offsite: u32,
    /// Devices already holding (or fetching) this blob, with their owner.
    pub holders: Vec<(DeviceId, OwnerId)>,
}

#[derive(Debug, Clone)]
pub struct PeerSpace {
    pub id: DeviceId,
    pub owner: OwnerId,
    /// Bytes still free under the grant/capacity that device gives us.
    pub free: u64,
    pub online: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Placement {
    pub hash: [u8; 32],
    pub device: DeviceId,
}

/// Compute placements to bring blobs toward their replica + offsite targets.
///
/// For each blob (worst deficit first): fill up to `target`, preferring
/// devices whose owner doesn't already hold a copy; then, if fewer than
/// `min_offsite` holders belong to other owners, add non-self placements
/// beyond `target` until satisfied. Ties break on id, so plans are stable.
pub fn plan(blobs: &[BlobNeed], peers: &[PeerSpace], self_owner: &OwnerId) -> Vec<Placement> {
    let mut free: BTreeMap<DeviceId, (OwnerId, u64)> = peers
        .iter()
        .filter(|p| p.online)
        .map(|p| (p.id, (p.owner, p.free)))
        .collect();

    let deficit = |b: &BlobNeed, holders: &[(DeviceId, OwnerId)]| {
        let base = (b.target as i64) - holders.len() as i64;
        let offsite = holders.iter().filter(|(_, o)| o != self_owner).count() as i64;
        let offsite_deficit = (b.min_offsite as i64) - offsite;
        base.max(offsite_deficit)
    };

    let mut ordered: Vec<&BlobNeed> = blobs
        .iter()
        .filter(|b| deficit(b, &b.holders) > 0)
        .collect();
    ordered.sort_by(|a, b| {
        deficit(b, &b.holders)
            .cmp(&deficit(a, &a.holders))
            .then_with(|| a.hash.cmp(&b.hash))
    });

    let mut out = Vec::new();
    for blob in ordered {
        let mut holders = blob.holders.clone();

        // Pick the best candidate for one replica: most free space first;
        // never a device already holding; prefer owner diversity, and when
        // `require_offsite`, only owners other than self.
        let mut place_one = |holders: &mut Vec<(DeviceId, OwnerId)>,
                             free: &mut BTreeMap<DeviceId, (OwnerId, u64)>,
                             require_offsite: bool|
         -> bool {
            let holder_devices: Vec<DeviceId> = holders.iter().map(|(d, _)| *d).collect();
            let holder_owners: Vec<OwnerId> = holders.iter().map(|(_, o)| *o).collect();
            let candidate = |diverse_only: bool| {
                free.iter()
                    .filter(|(id, (owner, avail))| {
                        !holder_devices.contains(id)
                            && *avail >= blob.size
                            && (!require_offsite || owner != self_owner)
                            && (!diverse_only || !holder_owners.contains(owner))
                    })
                    .max_by(|(id_a, (_, a)), (id_b, (_, b))| a.cmp(b).then_with(|| id_b.cmp(id_a)))
                    .map(|(id, (owner, _))| (*id, *owner))
            };
            // Owner-diverse first; same-owner-different-device as fallback.
            let picked = candidate(true).or_else(|| candidate(false));
            let Some((device, owner)) = picked else {
                return false;
            };
            free.get_mut(&device).unwrap().1 -= blob.size;
            holders.push((device, owner));
            out.push(Placement {
                hash: blob.hash,
                device,
            });
            true
        };

        // Fill the replica target.
        while (holders.len() as u32) < blob.target {
            if !place_one(&mut holders, &mut free, false) {
                break;
            }
        }
        // Then guarantee off-site copies (may exceed target).
        while (holders.iter().filter(|(_, o)| o != self_owner).count() as u32) < blob.min_offsite {
            if !place_one(&mut holders, &mut free, true) {
                break;
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    const ME: OwnerId = [0xAA; 32];

    fn peer(n: u8, owner: u8, free: u64) -> PeerSpace {
        PeerSpace {
            id: [n; 32],
            owner: [owner; 32],
            free,
            online: true,
        }
    }

    fn blob(n: u8, size: u64, target: u32, min_offsite: u32, holders: &[(u8, u8)]) -> BlobNeed {
        BlobNeed {
            hash: [n; 32],
            size,
            target,
            min_offsite,
            holders: holders.iter().map(|(d, o)| ([*d; 32], [*o; 32])).collect(),
        }
    }

    #[test]
    fn never_places_twice_on_same_device() {
        let placements = plan(
            &[blob(1, 100, 3, 0, &[])],
            &[peer(1, 1, 1000), peer(2, 2, 1000)],
            &ME,
        );
        assert_eq!(placements.len(), 2);
        assert_ne!(placements[0].device, placements[1].device);
    }

    #[test]
    fn prefers_owner_diversity() {
        // Two devices of owner 1 (more free space) and one of owner 2:
        // a 2-replica blob should use owners 1 AND 2, not 1's two devices.
        let peers = [peer(1, 1, 10_000), peer(2, 1, 9_000), peer(3, 2, 1_000)];
        let placements = plan(&[blob(1, 100, 2, 0, &[])], &peers, &ME);
        let owners: Vec<u8> = placements
            .iter()
            .map(|p| peers.iter().find(|x| x.id == p.device).unwrap().owner[0])
            .collect();
        assert_eq!(placements.len(), 2);
        assert!(
            owners.contains(&1) && owners.contains(&2),
            "owners used: {owners:?}"
        );
    }

    #[test]
    fn falls_back_to_same_owner_devices() {
        // Only owner 1 has capacity for the second replica.
        let peers = [peer(1, 1, 10_000), peer(2, 1, 10_000)];
        let placements = plan(&[blob(1, 100, 2, 0, &[])], &peers, &ME);
        assert_eq!(
            placements.len(),
            2,
            "should use two devices of the same owner"
        );
    }

    #[test]
    fn min_offsite_forces_non_self_copy() {
        // My own NAS (owner ME) could hold everything; min_offsite=1 must
        // still land one copy on the friend's box.
        let my_nas = PeerSpace {
            id: [1; 32],
            owner: ME,
            free: 1_000_000,
            online: true,
        };
        let friend = peer(2, 2, 1_000_000);
        let placements = plan(&[blob(1, 100, 1, 1, &[])], &[my_nas, friend], &ME);
        assert!(
            placements.iter().any(|p| p.device == [2; 32]),
            "no off-site copy placed: {placements:?}"
        );
    }

    #[test]
    fn min_offsite_satisfied_no_extra_placement() {
        let friend_holds = blob(1, 100, 1, 1, &[(2, 2)]);
        let placements = plan(
            &[friend_holds],
            &[PeerSpace {
                id: [1; 32],
                owner: ME,
                free: 1_000_000,
                online: true,
            }],
            &ME,
        );
        assert!(placements.is_empty());
    }

    #[test]
    fn respects_free_space_and_offline() {
        let peers = [
            peer(1, 1, 1000),
            PeerSpace {
                id: [2; 32],
                owner: [2; 32],
                free: 1_000_000,
                online: false,
            },
            peer(3, 3, 50),
        ];
        let placements = plan(&[blob(1, 600, 3, 0, &[])], &peers, &ME);
        assert_eq!(
            placements,
            vec![Placement {
                hash: [1; 32],
                device: [1; 32]
            }]
        );
    }

    #[test]
    fn deterministic() {
        let blobs: Vec<BlobNeed> = (0..10).map(|i| blob(i, 100, 2, 1, &[])).collect();
        let peers = vec![peer(1, 1, 10_000), peer(2, 2, 10_000), peer(3, 3, 10_000)];
        assert_eq!(plan(&blobs, &peers, &ME), plan(&blobs, &peers, &ME));
    }
}
