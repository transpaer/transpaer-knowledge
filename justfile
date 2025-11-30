# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

E := "7"
S := "0"
C := "0"
T := "0"

origin := "../data/origin"
meta := "../data/meta"
support := "../data/support"
substrate0 := "../data/substrate"
library := "../data/library"
cache := "../data/cache-" + E
substrate := "../data/substrate-" + E + "-" + S
coagulate := "../data/coagulate-" + E + "-" + S + "-" + C
target := "../data/target-" + E + "-" + S + "-" + C + "-" + T

kickstart:
    #cargo run --release --bin transpaer-lab -- extract \
    #    --origin {{origin}} \
    #    --cache {{cache}}

    cargo run --release --bin transpaer-lab -- condense \
        --group immediate \
        --origin {{origin}} \
        --meta {{meta}} \
        --support {{support}} \
        --cache {{cache}} \
        --substrate {{substrate}}

    cp -a {{substrate0}}/* {{substrate}}

    cargo run --release --bin transpaer-lab -- filter \
        --origin {{origin}} \
        --meta {{meta}} \
        --cache {{cache}} \
        --substrate {{substrate}}

    cargo run --release --bin transpaer-lab -- condense \
        --group filtered \
        --origin {{origin}} \
        --meta {{meta}} \
        --support {{support}} \
        --cache {{cache}} \
        --substrate {{substrate}}

    cargo run --release --bin transpaer-lab -- coagulate \
       --substrate {{substrate}} \
       --coagulate {{coagulate}}

    cargo run --release --bin transpaer-lab -- crystalize \
        --substrate {{substrate}} \
        --coagulate {{coagulate}} \
        --target {{target}}

    cargo run --release --bin transpaer-lab -- oxidize \
        --support {{support}} \
        --library {{library}} \
        --target {{target}}

    cargo run --release --bin transpaer-lab -- update \
        --origin {{origin}} \
        --cache {{cache}} \
        --substrate {{substrate}} \
        --meta {{meta}}

    echo DONE
