package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"voice-timeline-renderer/internal/checkflow"
)

func main() {
	workflow := flag.String("workflow", "", "workflow identifier to execute")
	workspaceRoot := flag.String("workspace", checkflow.DefaultWorkspaceRoot,
		"directory that holds per-run verification workspaces (failed runs are always retained here)")
	keepOnSuccess := flag.Bool("keep", false, "retain the run workspace even when the workflow passes")
	flag.Parse()
	if *workflow == "" {
		fmt.Fprintln(os.Stderr, "-workflow is required")
		os.Exit(2)
	}
	err := checkflow.Run(context.Background(), *workflow, checkflow.RunOptions{
		WorkspaceRoot: *workspaceRoot,
		KeepOnSuccess: *keepOnSuccess,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "workflow %s failed: %v\n", *workflow, err)
		os.Exit(1)
	}
	fmt.Printf("workflow %s passed\n", *workflow)
}
