# tiny-lsm
A tiny Log-Structured Merge Tree (LSM Tree) implementation in Rust for educational purposes.

I sort of messed up in naming the project since it really should be called tiny-lsmt or something like that, but oh well.

## Background
This repository is a companion to a talk I'm going to be giving at the Rust NYC 2025 Unconf. The slides will be made available soon.

The main branch has several key functions removed and replaced with `todo!()` macros since the second half of the session is going to be an interactive coding session where people can clone the repo and implement the missing pieces. 

If you get stuck there is a `HINTS.md` file that has some guidance on how to implement the missing pieces, and a complete implentation is available in the `solution` branch.

## Running Tests
To run the tests, simply execute:
```bash
cargo test
```

## License
This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

