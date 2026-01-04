// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

fn emit_build_info() -> Result<(), ()> {
    let build =
        vergen_gix::BuildBuilder::all_build().map_err(|e| eprintln!("Vergen builder: {e}"))?;
    let git = vergen_gix::GixBuilder::all_git().map_err(|e| eprintln!("Vergen git: {e}"))?;
    vergen_gix::Emitter::default()
        .add_instructions(&build)
        .map_err(|e| eprintln!("Vergen build instruction: {e}"))?
        .add_instructions(&git)
        .map_err(|e| eprintln!("Vergen git instruction: {e}"))?
        .emit()
        .map_err(|e| eprintln!("Vergen emit: {e}"))?;
    Ok(())
}

fn main() {
    emit_build_info().expect("Failed emiting build info")
}
