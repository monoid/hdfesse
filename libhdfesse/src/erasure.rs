/*
   Copyright 2021 Ivan Boldyrev

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
use crate::status::{EcSchema, ErasureCodingPolicy};

pub const DUMMY_CODEC_NAME: &str = "dummy";
pub const RS_CODEC_NAME: &str = "rs";
pub const RS_LEGACY_CODEC_NAME: &str = "rs-legacy";
pub const XOR_CODEC_NAME: &str = "xor";
pub const HHXOR_CODEC_NAME: &str = "hhxor";
pub const REPLICATION_CODEC_NAME: &str = "replication";

pub const USER_DEFINED_POLICY_START_ID: u8 = 64;
pub const REPLICATION_POLICY_ID: u8 = 0;
pub const REPLICATION_POLICY_NAME: &str = REPLICATION_CODEC_NAME;

const DEFAULT_CELLSIZE: u32 = 1024 * 1024;

lazy_static::lazy_static! {
    static ref RS_6_3_SCHEMA: EcSchema = EcSchema {
        codec_name: RS_CODEC_NAME.into(),
        data_units: 6,
        parity_units: 3,
        options: Default::default(),
    };

    static ref RS_3_2_SCHEMA: EcSchema = EcSchema {
        codec_name: RS_CODEC_NAME.into(),
        data_units: 3,
        parity_units: 2,
        options: Default::default(),
    };

    static ref RS_6_3_LEGACY_SCHEMA: EcSchema = EcSchema {
        codec_name: RS_LEGACY_CODEC_NAME.into(),
        data_units: 6,
        parity_units: 3,
        options: Default::default(),
    };

    static ref XOR_2_1_SCHEMA: EcSchema = EcSchema {
        codec_name: XOR_CODEC_NAME.into(),
        data_units: 2,
        parity_units: 1,
        options: Default::default(),
    };

    static ref RS_10_4_SCHEMA: EcSchema = EcSchema {
        codec_name: RS_CODEC_NAME.into(),
        data_units: 10,
        parity_units: 4,
        options: Default::default(),
    };

    static ref REPLICATION_1_2_SCHEMA: EcSchema = EcSchema {
        codec_name: REPLICATION_CODEC_NAME.into(),
        data_units: 1,
        parity_units: 2,
        options: Default::default(),
    };

    static ref SYS_POLICY1: ErasureCodingPolicy = ErasureCodingPolicy {
        name: RS_6_3_SCHEMA.codec_name.clone(),
        schema: RS_6_3_SCHEMA.clone(),
        cell_size: DEFAULT_CELLSIZE,
        id: 1,
    };

    static ref SYS_POLICY2: ErasureCodingPolicy = ErasureCodingPolicy {
        name: RS_3_2_SCHEMA.codec_name.clone(),
        schema: RS_3_2_SCHEMA.clone(),
        cell_size: DEFAULT_CELLSIZE,
        id: 2,
    };

    static ref SYS_POLICY3: ErasureCodingPolicy = ErasureCodingPolicy {
        name: RS_6_3_LEGACY_SCHEMA.codec_name.clone(),
        schema: RS_6_3_LEGACY_SCHEMA.clone(),
        cell_size: DEFAULT_CELLSIZE,
        id: 3,
    };

    static ref SYS_POLICY4: ErasureCodingPolicy = ErasureCodingPolicy {
        name: XOR_2_1_SCHEMA.codec_name.clone(),
        schema: XOR_2_1_SCHEMA.clone(),
        cell_size: DEFAULT_CELLSIZE,
        id: 4,
    };

    static ref SYS_POLICY5: ErasureCodingPolicy = ErasureCodingPolicy {
        name: RS_10_4_SCHEMA.codec_name.clone(),
        schema: RS_10_4_SCHEMA.clone(),
        cell_size: DEFAULT_CELLSIZE,
        id: 5,
    };

    static ref REPLICATION_POLICY: ErasureCodingPolicy = ErasureCodingPolicy {
        name: REPLICATION_POLICY_NAME.into(),
        schema: REPLICATION_1_2_SCHEMA.clone(),
        cell_size: DEFAULT_CELLSIZE,
        id: REPLICATION_POLICY_ID,
    };

    static ref SYS_POLICIES: Box<[&'static ErasureCodingPolicy]> = vec![
        &*SYS_POLICY1, &*SYS_POLICY2, &*SYS_POLICY3, &*SYS_POLICY4, &*SYS_POLICY5,
    ].into_boxed_slice();
}

pub struct SystemErasureCodingPolicy {}

// TODO: has it to return an Option<Arc<ErasureCodingPolicy>>?
impl SystemErasureCodingPolicy {
    pub fn get_by_id(id: u8) -> Option<&'static ErasureCodingPolicy> {
        SYS_POLICIES.iter().copied().find(|item| item.id == id)
    }
}
