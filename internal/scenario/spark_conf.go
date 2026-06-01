package scenario

import (
	"sort"
	"strings"

	"github.com/opay-bigdata/spark-cli/internal/model"
)

type SparkConfRow struct {
	Key        string `json:"key"`
	Value      string `json:"value"`
	Category   string `json:"category"`
	Importance string `json:"importance"`
	TuningHint string `json:"tuning_hint"`
}

type SparkConfSummary struct {
	Total            int                      `json:"total"`
	Important        int                      `json:"important"`
	MissingImportant []string                 `json:"missing_important,omitempty"`
	ParameterHints   []SparkConfParameterHint `json:"parameter_hints,omitempty"`
}

type SparkConfParameterHint struct {
	Problem string   `json:"problem"`
	Keys    []string `json:"keys"`
	Note    string   `json:"note"`
}

type sparkConfMeta struct {
	category string
	hint     string
}

func SparkConfColumns() []string {
	return []string{"key", "value", "category", "importance", "tuning_hint"}
}

func SparkConf(app *model.Application) ([]SparkConfRow, SparkConfSummary) {
	summary := SparkConfSummary{
		Total:          len(app.SparkConf),
		ParameterHints: sparkConfParameterHints(),
	}
	if len(app.SparkConf) == 0 {
		summary.MissingImportant = importantSparkConfKeys()
		return nil, summary
	}

	rows := make([]SparkConfRow, 0, len(app.SparkConf))
	for key, value := range app.SparkConf {
		meta := classifySparkConf(key)
		importance := "normal"
		if _, ok := importantSparkConfMeta()[key]; ok {
			importance = "important"
			summary.Important++
		}
		rows = append(rows, SparkConfRow{
			Key:        key,
			Value:      value,
			Category:   meta.category,
			Importance: importance,
			TuningHint: meta.hint,
		})
	}
	sort.SliceStable(rows, func(i, j int) bool {
		if rows[i].Importance != rows[j].Importance {
			return rows[i].Importance == "important"
		}
		if rows[i].Category != rows[j].Category {
			return sparkConfCategoryRank(rows[i].Category) < sparkConfCategoryRank(rows[j].Category)
		}
		return rows[i].Key < rows[j].Key
	})
	for _, key := range importantSparkConfKeys() {
		if _, ok := app.SparkConf[key]; !ok {
			summary.MissingImportant = append(summary.MissingImportant, key)
		}
	}
	return rows, summary
}

func classifySparkConf(key string) sparkConfMeta {
	if meta, ok := importantSparkConfMeta()[key]; ok {
		return meta
	}
	switch {
	case strings.HasPrefix(key, "spark.driver."):
		return sparkConfMeta{category: "driver"}
	case strings.HasPrefix(key, "spark.executor."):
		return sparkConfMeta{category: "executor"}
	case strings.HasPrefix(key, "spark.dynamicAllocation."):
		return sparkConfMeta{category: "dynamic_allocation"}
	case strings.Contains(key, "Broadcast") || strings.Contains(key, "broadcast"):
		return sparkConfMeta{category: "broadcast"}
	case strings.HasPrefix(key, "spark.sql.adaptive."):
		return sparkConfMeta{category: "adaptive"}
	case strings.HasPrefix(key, "spark.sql.shuffle."):
		return sparkConfMeta{category: "shuffle"}
	case strings.HasPrefix(key, "spark.memory.") || strings.HasPrefix(key, "spark.memoryOverhead."):
		return sparkConfMeta{category: "memory"}
	case strings.HasPrefix(key, "spark.sql."):
		return sparkConfMeta{category: "sql"}
	case strings.HasPrefix(key, "spark.app."):
		return sparkConfMeta{category: "app"}
	default:
		return sparkConfMeta{category: "other"}
	}
}

func importantSparkConfMeta() map[string]sparkConfMeta {
	return map[string]sparkConfMeta{
		"spark.driver.memory": {
			category: "driver",
			hint:     "driver 端 planning、collect、broadcast 卡顿时优先核对。",
		},
		"spark.driver.memoryOverhead": {
			category: "driver",
			hint:     "driver 容器非堆内存不足时会放大 broadcast / UI / RPC 等等待。",
		},
		"spark.executor.memory": {
			category: "executor",
			hint:     "executor GC、spill 或 task OOM 时优先核对。",
		},
		"spark.executor.memoryOverhead": {
			category: "executor",
			hint:     "shuffle、native reader 或 Python/非堆内存压力高时优先核对。",
		},
		"spark.executor.cores": {
			category: "executor",
			hint:     "影响 task 并发与单 executor 内存竞争。",
		},
		"spark.executor.instances": {
			category: "executor",
			hint:     "静态 executor 数；dynamic allocation 开启时通常为 0 或未设置。",
		},
		"spark.dynamicAllocation.enabled": {
			category: "dynamic_allocation",
			hint:     "executor 供给不足、启动慢或 stage 等待时优先核对。",
		},
		"spark.dynamicAllocation.maxExecutors": {
			category: "dynamic_allocation",
			hint:     "限制最大并行度，影响大 shuffle / 宽表扫描。",
		},
		"spark.sql.shuffle.partitions": {
			category: "shuffle",
			hint:     "spill、tiny task、shuffle 分区过粗/过细时优先核对。",
		},
		"spark.sql.adaptive.advisoryPartitionSizeInBytes": {
			category: "shuffle",
			hint:     "AQE 合并分区的目标大小；spill 或单 partition 过大时可调小。",
		},
		"spark.sql.adaptive.enabled": {
			category: "adaptive",
			hint:     "AQE 总开关，影响 coalesce、skew join、join 策略调整。",
		},
		"spark.sql.adaptive.skewJoin.enabled": {
			category: "adaptive",
			hint:     "数据倾斜长尾时优先核对。",
		},
		"spark.sql.adaptive.skewJoin.skewedPartitionFactor": {
			category: "adaptive",
			hint:     "倾斜 join 检测倍数阈值；过大可能漏拆倾斜分区。",
		},
		"spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": {
			category: "adaptive",
			hint:     "倾斜 join 检测大小阈值；过大可能漏拆倾斜分区。",
		},
		"spark.sql.autoBroadcastJoinThreshold": {
			category: "broadcast",
			hint:     "broadcast 卡顿、driver OOM 或误广播大表时优先核对，可临时设 -1 验证。",
		},
		"spark.sql.broadcastTimeout": {
			category: "broadcast",
			hint:     "broadcast 等待超时；-1 会让问题表现为长时间挂起。",
		},
		"spark.memory.fraction": {
			category: "memory",
			hint:     "影响 execution/storage 内存比例，GC/spill 时结合 executor memory 看。",
		},
	}
}

func importantSparkConfKeys() []string {
	keys := make([]string, 0, len(importantSparkConfMeta()))
	for key := range importantSparkConfMeta() {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func sparkConfCategoryRank(category string) int {
	switch category {
	case "driver":
		return 0
	case "broadcast":
		return 1
	case "executor":
		return 2
	case "dynamic_allocation":
		return 3
	case "shuffle":
		return 4
	case "adaptive":
		return 5
	case "memory":
		return 6
	case "sql":
		return 7
	case "app":
		return 8
	default:
		return 9
	}
}

func sparkConfParameterHints() []SparkConfParameterHint {
	return []SparkConfParameterHint{
		{
			Problem: "driver_broadcast_wait",
			Keys: []string{
				"spark.driver.memory",
				"spark.driver.memoryOverhead",
				"spark.sql.autoBroadcastJoinThreshold",
				"spark.sql.broadcastTimeout",
			},
			Note: "idle_stage 或 broadcast-exchange 卡顿时先看这些配置。",
		},
		{
			Problem: "shuffle_spill_or_large_partition",
			Keys: []string{
				"spark.sql.shuffle.partitions",
				"spark.sql.adaptive.advisoryPartitionSizeInBytes",
				"spark.executor.memory",
			},
			Note: "disk_spill 或 shuffle_read_mb_per_task 过大时结合这些配置给建议。",
		},
		{
			Problem: "data_skew",
			Keys: []string{
				"spark.sql.adaptive.enabled",
				"spark.sql.adaptive.skewJoin.enabled",
				"spark.sql.adaptive.skewJoin.skewedPartitionFactor",
				"spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes",
			},
			Note: "data_skew 命中时用这些配置判断 AQE 是否能拆倾斜分区。",
		},
		{
			Problem: "gc_pressure",
			Keys: []string{
				"spark.executor.memory",
				"spark.executor.memoryOverhead",
				"spark.memory.fraction",
			},
			Note: "GC 压力高时用这些配置区分堆内存不足、非堆不足或分区过粗。",
		},
	}
}
