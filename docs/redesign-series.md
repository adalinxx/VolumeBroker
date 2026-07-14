# Redesign series dependency

This PR is stacked on the cashew targeted-Volume storage branch. The temporary branch requirement in `Package.swift` exists so CI validates both sides of the traversal/storage contract together.

Before merge, publish the cashew change and replace the branch dependency with the resulting release version. The top-level lattice-node PR consumes both branches as the full-stack integration gate.
