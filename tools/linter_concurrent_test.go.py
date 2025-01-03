import os
import re


def add_parallel_to_tests(root_dir):
    excluded_folder = "/test"
    for dirpath, dirnames, filenames in os.walk(root_dir):
        # Skip the excluded folder
        if excluded_folder in dirpath:
            continue

        for filename in filenames:
            if filename.endswith(".go"):
                file_path = os.path.join(dirpath, filename)
                process_go_file(file_path)


def process_go_file(file_path):
    with open(file_path, "r") as file:
        lines = file.readlines()

    updated_lines = []
    modified = False

    test_function_pattern = re.compile(r"^func Test\w+\(t \*testing\.T\) {$")
    for i, line in enumerate(lines):
        # Check if line matches test function declaration
        if test_function_pattern.match(line.strip()):
            # Find the start of the current test function
            test_start = i
            test_end = find_test_function_end(lines, test_start)

            # Check for `// no parallel` comment as the first line in the function
            if test_start > 0 and lines[test_start - 1].strip() == "// no parallel":
                updated_lines.append(line)
                continue

            # Check for existing `t.Parallel()` within the function body
            has_parallel = any("t.Parallel()" in lines[j] for j in range(test_start + 1, test_end))
            has_setenv = any(
                "t.Setenv" in lines[j] or "os.Setenv" in lines[j]
                for j in range(test_start + 1, test_end)
            )

            if not has_parallel and not has_setenv:
                # Add `t.Parallel()` as the first line of the function body
                updated_lines.append(line)
                updated_lines.append("    t.Parallel()\n")
                modified = True
            else:
                updated_lines.append(line)
        else:
            updated_lines.append(line)

    if modified:
        with open(file_path, "w") as file:
            file.writelines(updated_lines)
        print(f"Updated file: {file_path}")


def find_test_function_end(lines, start_index):
    """
    Finds the end of a Go test function by identifying the closing brace.
    Assumes that braces are properly balanced.
    """
    open_braces = 0
    for i in range(start_index, len(lines)):
        open_braces += lines[i].count("{")
        open_braces -= lines[i].count("}")
        if open_braces == 0:
            return i + 1
    return len(lines)  # Return EOF if function end not found


if __name__ == "__main__":
    root_dir = os.getcwd()
    add_parallel_to_tests(root_dir)
