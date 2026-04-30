// Copyright (c) 2026 OPay Bigdata.
// SPDX-License-Identifier: MIT
package main

import (
	"os"

	"github.com/opay-bigdata/spark-cli/cmd"
)

func main() {
	os.Exit(cmd.Execute())
}
