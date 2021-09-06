// clusterintegrationtest acts as a test package to provide a compoenent test
// spanning multiple parts of the application, including everything that's
// required for a distributed setup. It thus acts like a mini "main" page and
// must be separated from the rest of the package to avoid circular import
// issues, etc.
package clusterintegrationtest
