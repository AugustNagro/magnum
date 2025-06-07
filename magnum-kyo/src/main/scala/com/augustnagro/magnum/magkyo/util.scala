package com.augustnagro.magnum.magkyo

import kyo.*

// TODO: Replace with https://github.com/getkyo/kyo/issues/1220 when resolved
inline def acquireReleaseWith[A, S1](
    acquire: => A < (S1 & IO)
)(release: A => Any < IO)[B, S2](
    use: A => B < S2
)(using Frame): B < (IO & S1 & S2) =
  IO(acquire).map(a => IO.ensure(release(a))(use(a)))
