package sync

import "strings"

// keyChecksum 参与对账比较的一条记录：业务键 + 关键字段指纹
type keyChecksum struct {
	Key      string
	Checksum string
}

// keyIter 按 Key 升序拉取记录，取完返回 (nil, nil)。
// 抽成函数类型是为了让归并算法与数据来源解耦：
// 生产环境背后是两个库的游标，测试里可以直接喂切片。
type keyIter func() (*keyChecksum, error)

// diffResult 归并比较的结论
type diffResult struct {
	Missing  int64
	Extra    int64
	Mismatch int64

	// 差异键抽样，超过上限只保留前 N 条，避免把整表差异灌进元数据库
	MissingKeys  []string
	ExtraKeys    []string
	MismatchKeys []string
	Truncated    bool
}

// mergeCompare 对两个按业务键升序的序列做归并比较。
//
// 用归并而不是"两边各查一次装进 map 再比"：后者内存占用随表增长，
// 百万级就要几百 MB。归并只需同时持有两侧各一行，内存 O(1)，
// 代价是要求两端严格同序——这也是查询里强制统一排序规则的原因。
//
// 三类差异的业务含义：
//   - Missing  源有目标无：漏同步，最严重
//   - Extra    目标有源无：多数是源端物理删除，watermark 增量感知不到
//   - Mismatch 键相同但关键字段不一致：同步过但内容旧了或转换有问题
func mergeCompare(src, tgt keyIter, sampleLimit int) (diffResult, error) {
	var d diffResult
	if sampleLimit <= 0 {
		sampleLimit = 100
	}

	sample := func(dst *[]string, key string) {
		if len(*dst) < sampleLimit {
			*dst = append(*dst, key)
			return
		}
		d.Truncated = true
	}

	s, err := src()
	if err != nil {
		return d, err
	}
	t, err := tgt()
	if err != nil {
		return d, err
	}

	for s != nil || t != nil {
		switch {
		case t == nil:
			// 目标端已读完，剩下的源记录全是漏同步
			d.Missing++
			sample(&d.MissingKeys, s.Key)
			if s, err = src(); err != nil {
				return d, err
			}

		case s == nil:
			// 源端已读完，剩下的目标记录全是多余
			d.Extra++
			sample(&d.ExtraKeys, t.Key)
			if t, err = tgt(); err != nil {
				return d, err
			}

		default:
			switch cmp := strings.Compare(s.Key, t.Key); {
			case cmp < 0:
				// 源键更小：目标端跳过了它
				d.Missing++
				sample(&d.MissingKeys, s.Key)
				if s, err = src(); err != nil {
					return d, err
				}
			case cmp > 0:
				// 目标键更小：源端没有它
				d.Extra++
				sample(&d.ExtraKeys, t.Key)
				if t, err = tgt(); err != nil {
					return d, err
				}
			default:
				// 键相同，比指纹
				if s.Checksum != t.Checksum {
					d.Mismatch++
					sample(&d.MismatchKeys, s.Key)
				}
				if s, err = src(); err != nil {
					return d, err
				}
				if t, err = tgt(); err != nil {
					return d, err
				}
			}
		}
	}

	return d, nil
}

// sliceIter 把切片包装成 keyIter，供测试与小数据量场景使用
func sliceIter(items []keyChecksum) keyIter {
	i := 0
	return func() (*keyChecksum, error) {
		if i >= len(items) {
			return nil, nil
		}
		v := items[i]
		i++
		return &v, nil
	}
}
