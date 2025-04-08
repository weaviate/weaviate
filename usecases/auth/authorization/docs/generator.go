//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

//go:build ignore
// +build ignore

package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
)

type authCall struct {
	Function string
	// Map verbs to their resources
	VerbResourceMap map[string][]string // key: verb, value: slice of resources
	FilePath        string
}

const mainDirPath = "../../../../"

func main() {
	var calls []authCall
	var totalFiles, skippedFiles, processedFiles int

	err := filepath.Walk(mainDirPath, func(path string, info os.FileInfo, err error) error {
		totalFiles++

		if err != nil {
			fmt.Fprintf(os.Stderr, "Error accessing path %s: %v\n", path, err)
			return nil
		}

		// Skip directories
		if info.IsDir() {
			if shouldSkipDir(path) {
				fmt.Fprintf(os.Stderr, "Skipping directory: %s\n", path)
				return filepath.SkipDir
			}
			return nil
		}

		// Skip non-go files, vendor, and test files
		if !strings.HasSuffix(path, ".go") ||
			strings.Contains(path, "/vendor/") ||
			strings.HasSuffix(path, "_test.go") {
			skippedFiles++
			return nil
		}

		fmt.Fprintf(os.Stderr, "Processing file: %s\n", path)
		processedFiles++

		// Read and parse the file
		fset := token.NewFileSet()
		content, err := os.ReadFile(path)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error reading %s: %v\n", path, err)
			return nil
		}

		node, err := parser.ParseFile(fset, path, content, 0)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error parsing %s: %v\n", path, err)
			return nil
		}

		// Visit all nodes in the AST
		ast.Inspect(node, func(n ast.Node) bool {
			if call, ok := n.(*ast.CallExpr); ok {
				if sel, ok := call.Fun.(*ast.SelectorExpr); ok {
					if sel.Sel.Name == "Authorize" {
						// Find the containing function
						var funcName string
						ast.Inspect(node, func(parent ast.Node) bool {
							if fn, ok := parent.(*ast.FuncDecl); ok {
								if fn.Pos() <= call.Pos() && call.End() <= fn.End() {
									// Skip test functions
									if !strings.HasPrefix(fn.Name.Name, "Test") {
										funcName = fn.Name.Name
									}
									return false
								}
							}
							return true
						})

						// Skip if no valid function name (e.g., test function)
						if funcName == "" {
							return true
						}

						if len(call.Args) >= 3 {
							verb := formatArg(call.Args[1])
							resource := formatArg(call.Args[2])

							verb = strings.TrimPrefix(verb, "&")
							verb = strings.TrimPrefix(verb, "authorization.")
							resource = strings.TrimPrefix(resource, "authorization.")

							// Check if we already have an entry for this function+file
							if idx, found := findOrCreateCall(calls, funcName, path); found {
								// Initialize map if needed
								if calls[idx].VerbResourceMap == nil {
									calls[idx].VerbResourceMap = make(map[string][]string)
								}
								// Add resource to verb's resource list if not already present
								if !contains(calls[idx].VerbResourceMap[verb], resource) {
									calls[idx].VerbResourceMap[verb] = append(calls[idx].VerbResourceMap[verb], resource)
								}
							} else {
								verbMap := make(map[string][]string)
								verbMap[verb] = []string{resource}
								calls = append(calls, authCall{
									Function:        funcName,
									VerbResourceMap: verbMap,
									FilePath:        path,
								})
							}
						}
					}
				}
			}
			return true
		})
		return nil
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error walking directory: %v\n", err)
		os.Exit(1)
	}

	// Print statistics
	fmt.Fprintf(os.Stderr, "\nStatistics:\n")
	fmt.Fprintf(os.Stderr, "Total files found: %d\n", totalFiles)
	fmt.Fprintf(os.Stderr, "Files skipped: %d\n", skippedFiles)
	fmt.Fprintf(os.Stderr, "Files processed: %d\n", processedFiles)

	// Count total auth calls by summing the length of VerbResourceMap for each function
	totalCalls := 0
	for _, call := range calls {
		totalCalls += len(call.VerbResourceMap)
	}
	fmt.Fprintf(os.Stderr, "Total Authorize calls found: %d\n", totalCalls)

	// Create and write to the markdown file
	f, err := os.Create("auth_calls.md")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating file: %v\n", err)
		os.Exit(1)
	}
	defer f.Close()

	// Write the table header
	fmt.Fprintln(f, "# Authorization Calls")
	fmt.Fprintln(f, "This document lists all authorization calls in the codebase.")

	// Add usage section
	fmt.Fprintln(f, "## Usage")
	fmt.Fprintln(f, "To regenerate this documentation, run the following commands from the repository root:")
	fmt.Fprintln(f, "```bash")
	fmt.Fprintln(f, "cd usecases/auth/authorization/docs")
	fmt.Fprintln(f, "go run generator.go")
	fmt.Fprintln(f, "```")

	// Continue with statistics section
	fmt.Fprintln(f, "## Statistics")
	fmt.Fprintf(f, "- Total files found: %d\n", totalFiles)
	fmt.Fprintf(f, "- Files processed: %d\n", processedFiles)
	fmt.Fprintf(f, "- Total Authorize calls found: %d\n\n", totalCalls)

	fmt.Fprintln(f, "| Function | File | Verb → Resources |")
	fmt.Fprintln(f, "|----------|------|-----------------|")

	// Write each call in table format
	for _, call := range calls {
		var mappings []string
		for verb, resources := range call.VerbResourceMap {
			mappings = append(mappings, fmt.Sprintf("%s → %s", verb, strings.Join(resources, ", ")))
		}
		fmt.Fprintf(f, "| %s | %s | %s |\n",
			call.Function,
			strings.TrimPrefix(call.FilePath, mainDirPath),
			strings.Join(mappings, "<br>"),
		)
	}

	fmt.Fprintf(os.Stderr, "Results written to auth_calls.md\n")
}

func shouldSkipDir(path string) bool {
	skippedDirs := []string{
		".git",
		"vendor",
		"node_modules",
		"dist",
		"build",
	}

	base := filepath.Base(path)
	for _, skip := range skippedDirs {
		if base == skip {
			return true
		}
	}
	return false
}

func formatArg(expr ast.Expr) string {
	switch v := expr.(type) {
	case *ast.SelectorExpr:
		if ident, ok := v.X.(*ast.Ident); ok {
			return fmt.Sprintf("%s.%s", ident.Name, v.Sel.Name)
		}
	case *ast.Ident:
		return v.Name
	case *ast.CallExpr:
		if sel, ok := v.Fun.(*ast.SelectorExpr); ok {
			if ident, ok := sel.X.(*ast.Ident); ok {
				return fmt.Sprintf("%s.%s", ident.Name, sel.Sel.Name)
			}
		}
	}
	return fmt.Sprintf("%v", expr)
}

func findOrCreateCall(calls []authCall, function, filePath string) (int, bool) {
	for i, call := range calls {
		if call.Function == function && call.FilePath == filePath {
			return i, true
		}
	}
	return 0, false
}

func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}
