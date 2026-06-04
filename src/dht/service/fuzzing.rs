// SPDX-FileCopyrightText: 2025 The superseedr Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

use std::time::{Duration, Instant};

use super::lifecycle::{DhtLifecycleAction, DhtLifecycleEffect, DhtLifecycleModel};

pub(crate) fn reduce_lifecycle_for_fuzzing(bytes: &[u8]) {
    let mut input = FuzzBytes::new(bytes);
    let base = Instant::now();
    let steps = usize::from(input.next_u8() % 64) + 1;

    for _ in 0..steps {
        match input.next_u8() % 8 {
            0 => {
                let now = instant_from_ms(base, input.next_u32());
                let due = instant_from_ms(base, input.next_u32());
                let active_user_lookup_count = usize::from(input.next_u8() % 4);
                let expected = if now >= due && active_user_lookup_count == 0 {
                    vec![DhtLifecycleEffect::RunStartupBootstrap]
                } else {
                    Vec::new()
                };

                assert_lifecycle_reduction(
                    DhtLifecycleAction::StartupBootstrapDue {
                        now,
                        due,
                        active_user_lookup_count,
                    },
                    expected,
                );
            }
            1 => {
                assert_lifecycle_reduction(
                    DhtLifecycleAction::StartupBootstrapSucceeded,
                    vec![DhtLifecycleEffect::ClearStartupBootstrapDue],
                );
            }
            2 => {
                let warning = input.next_ascii();
                let retry_at = instant_from_ms(base, input.next_u32());
                assert_lifecycle_reduction(
                    DhtLifecycleAction::StartupBootstrapFailed {
                        warning: warning.clone(),
                        retry_at,
                    },
                    vec![
                        DhtLifecycleEffect::RecordRuntimeWarning {
                            warning,
                            publish_status: false,
                        },
                        DhtLifecycleEffect::SetStartupBootstrapDue(retry_at),
                    ],
                );
            }
            3 => {
                let active_user_lookup_count = if input.next_u8() & 1 == 0 {
                    None
                } else {
                    Some(usize::from(input.next_u8() % 4))
                };
                let expected = if active_user_lookup_count == Some(0) {
                    vec![DhtLifecycleEffect::RunMaintenance]
                } else {
                    Vec::new()
                };

                assert_lifecycle_reduction(
                    DhtLifecycleAction::MaintenanceTick {
                        active_user_lookup_count,
                    },
                    expected,
                );
            }
            4 => {
                let warning = input.next_ascii();
                assert_lifecycle_reduction(
                    DhtLifecycleAction::MaintenanceFailed {
                        warning: warning.clone(),
                    },
                    vec![DhtLifecycleEffect::RecordRuntimeWarning {
                        warning,
                        publish_status: true,
                    }],
                );
            }
            5 => {
                assert_lifecycle_reduction(
                    DhtLifecycleAction::HealthTick,
                    vec![
                        DhtLifecycleEffect::PublishStatus,
                        DhtLifecycleEffect::ExpireRecentUniquePeers,
                        DhtLifecycleEffect::SaveRuntimeState,
                    ],
                );
            }
            6 => {
                let warning = input.next_ascii();
                assert_lifecycle_reduction(
                    DhtLifecycleAction::RuntimeStepFailed {
                        warning: warning.clone(),
                    },
                    vec![DhtLifecycleEffect::RecordRuntimeWarning {
                        warning,
                        publish_status: true,
                    }],
                );
            }
            _ => {
                assert_lifecycle_reduction(
                    DhtLifecycleAction::Shutdown,
                    vec![DhtLifecycleEffect::SaveRuntimeState],
                );
            }
        }
    }
}

fn assert_lifecycle_reduction(action: DhtLifecycleAction, expected: Vec<DhtLifecycleEffect>) {
    let reduction = DhtLifecycleModel::update(action);
    assert_eq!(reduction.effects, expected);
}

fn instant_from_ms(base: Instant, value: u32) -> Instant {
    base + Duration::from_millis(u64::from(value % 600_000))
}

struct FuzzBytes<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> FuzzBytes<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn next_u8(&mut self) -> u8 {
        let byte = self.bytes.get(self.offset).copied().unwrap_or_default();
        self.offset = self.offset.saturating_add(1);
        byte
    }

    fn next_u32(&mut self) -> u32 {
        u32::from_be_bytes([
            self.next_u8(),
            self.next_u8(),
            self.next_u8(),
            self.next_u8(),
        ])
    }

    fn next_ascii(&mut self) -> String {
        let len = usize::from(self.next_u8() % 32);
        let mut text = String::with_capacity(len);
        for _ in 0..len {
            let byte = b'a' + (self.next_u8() % 26);
            text.push(char::from(byte));
        }
        text
    }
}
