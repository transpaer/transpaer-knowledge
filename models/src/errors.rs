// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#[derive(Clone, Debug, thiserror::Error)]
pub enum ModelsError {
    #[error("Significances from `{substrate_name}` substrate found more than once")]
    CombineSignificances { substrate_name: String },
}
