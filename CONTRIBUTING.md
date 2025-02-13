# Contributing

## Did you find a bug?

* **Ensure the bug was not already reported** by searching on GitHub under [Issues](https://github.com/dentiny/duck-read-cache-fs/issues).
* If you're unable to find an open issue addressing the problem, [open a new one](https://github.com/dentiny/duck-read-cache-fs/issues/new/choose). Be sure to include a **title and clear description**, as much relevant information as possible, and a **code sample** or an **executable test case** demonstrating the expected behavior that is not occurring.

## Did you write a patch that fixes a bug?

* Great!
* If possible, add a unit test case to make sure the issue does not occur again.
* Make sure you run the code formatter (`make format-all`).
* Open a new GitHub pull request with the patch.
* Ensure the PR description clearly describes the problem and solution. Include the relevant issue number if applicable.

## Pull Requests

* Do not commit/push directly to the main branch. Instead, create a fork and file a pull request.
* When maintaining a branch, merge frequently with the main.
* When maintaining a branch, submit pull requests to the main frequently.
* If you are working on a bigger issue try to split it up into several smaller issues.
* Please do not open "Draft" pull requests. Rather, use issues or discussion topics to discuss whatever needs discussing.
* We reserve full and final discretion over whether or not we will merge a pull request. Adhering to these guidelines is not a complete guarantee that your pull request will be merged.

## Building

* To build the project, run `CMAKE_BUILD_PARALLEL_LEVEL=$(nproc) make <debug>`.

## Formatting

* Use tabs for indentation, spaces for alignment.
* Lines should not exceed 120 columns.
* `clang-format` enforce these rules automatically, use `make format-all` to run the formatter.
* The project also comes with an [`.editorconfig` file](https://editorconfig.org/) that corresponds to these rules.

### DuckDB C++ Guidelines

* Do not use `malloc`, prefer the use of smart pointers. Keywords `new` and `delete` are a code smell.
* Strongly prefer the use of `unique_ptr` over `shared_ptr`, only use `shared_ptr` if you **absolutely** have to.
* Use `const` whenever possible.
* Do **not** import namespaces (e.g. `using std`).
* All functions in source files in the core (`src` directory) should be part of the `pgduckdb` namespace.
* When overriding a virtual method, avoid repeating virtual and always use `override` or `final`.
* Use `[u]int(8|16|32|64)_t` instead of `int`, `long`, `uint` etc. Use `idx_t` instead of `size_t` for offsets/indices/counts of any kind.
* Prefer using references over pointers as arguments.
* Use `const` references for arguments of non-trivial objects (e.g. `std::vector`, ...).
* Use C++11 for loops when possible: `for (const auto& item : items) {...}`
* Use braces for indenting `if` statements and loops. Avoid single-line if statements and loops, especially nested ones.
* **Class Layout:** Start out with a `public` block containing the constructor and public variables, followed by a `public` block containing public methods of the class. After that follow any private functions and private variables. For example:
    ```cpp
    class MyClass {
    public:
    	MyClass();

    	int my_public_variable;

    public:
    	void MyFunction();

    private:
    	void MyPrivateFunction();

    private:
    	int my_private_variable;
    };
    ```
* Avoid [unnamed magic numbers](https://en.wikipedia.org/wiki/Magic_number_(programming)). Instead, use named variables that are stored in a `constexpr`.
* [Return early](https://medium.com/swlh/return-early-pattern-3d18a41bba8). Avoid deep nested branches.
* Do not include commented out code blocks in pull requests.

## Error Handling

* Use exceptions **only** when an error is encountered that terminates a query (e.g. parser error, table not found). Exceptions should only be used for **exceptional** situations. For regular errors that do not break the execution flow (e.g. errors you **expect** might occur) use a return value instead.
* There are two distinct parts of the code where error handling is done very differently: The code that executes before we enter DuckDB execution engine (e.g. initial part of the planner hook) and the part that gets executed inside the duckdb execution engine. Below are rules for how to handle errors in both parts of the code. Not following these guidelines can cause crashes, memory leaks and other unexpected problems.
* Before we enter the DuckDB exection engine no exceptions should ever be thrown here. In cases where you would want to throw an exception here, use `elog(ERROR, ...)`. Any C++ code that might throw an exception is also problematic. Since C++ throws exceptions on allocation failures, this covers lots of C++ APIs. So try to use Postgres datastructures instead of C++ ones whenever possible (e.g. use `List` instead of `Vec`)
* Inside the duckdb execution engine the opposite is true. `elog(ERROR, ...)` should never be used there, use exceptions instead.
* Use PostgreSQL *elog* API can be used to report non-fatal messages back to user. Using *ERROR* is strictly forbiden to use in code that is executed inside the duckdb engine.
* Calling PostgreSQL native functions from within DuckDB execution needs **extreme care**. Pretty much non of these functions are thread-safe, and they might throw errors using `elog(ERROR, ...)`. If you've solved the thread-safety issue by taking a lock (or by carefully asserting that the actual code is thread safe), then you can use *PostgresFunctionGuard* to solve the `elog(ERROR, ...) problem. *PostgresFunctionGuard* will correctly handle *ERROR* log messages that could be emmited from these functions.
* Try to add test cases that trigger exceptions. If an exception cannot be easily triggered using a test case then it should probably be an assertion. This is not always true (e.g. out of memory errors are exceptions, but are very hard to trigger).
* Use `D_ASSERT` to assert. Use **assert** only when failing the assert means a programmer error. Assert should never be triggered by user input. Avoid code like `D_ASSERT(a > b + 3);` without comments or context.
* Assert liberally, but make it clear with comments next to the assert what went wrong when the assert is triggered.

## Naming Conventions

* Choose descriptive names. Avoid single-letter variable names.
* Files: lowercase separated by underscores, e.g., abstract_operator.cpp
* Types (classes, structs, enums, typedefs, using): CamelCase starting with uppercase letter, e.g., BaseColumn
* Variables: lowercase separated by underscores, e.g., chunk_size
* Functions: CamelCase starting with uppercase letter, e.g., GetChunk
* Avoid `i`, `j`, etc. in **nested** loops. Prefer to use e.g. **column_idx**, **check_idx**. In a **non-nested** loop it is permissible to use **i** as iterator index.
* These rules are partially enforced by `clang-tidy`.
