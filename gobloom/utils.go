// Copyright 2024 geebytes. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package gobloom

import "math"

// static double calc_bpe(double error) {
//     static const double denom = 0.480453013918201; // ln(2)^2
//     double num = log(error);
//
//     double bpe = -(num / denom);
//     if (bpe < 0) {
//         bpe = -bpe;
//     }
//     return bpe;
// }
// 这个`calc_bpe`函数是在布隆过滤器的上下文中用来计算“每个元素的比特数”（Bits Per Element），这是为了达到预定的错误率（false positive rate）。函数的输入是期望的错误率（`error`），输出是为了达到这个错误率所需要的每个元素的比特数。
//
// 布隆过滤器是一种空间效率很高的数据结构，用于测试一个元素是否在一个集合中。它允许有一定的错误率（false positives），但不会产生错误的否定（false negatives）。换句话说，布隆过滤器可能会告诉你一个元素在集合中，即使它不在；但如果它告诉你一个元素不在集合中，那么这个元素确实不在集合中。
//
// 函数中的`denom`（分母）是$\ln(2)^2$的值。这个值来自于布隆过滤器的最优比特数（每个元素）的计算公式，该公式旨在最小化在给定错误率下的空间使用。具体来说，最优的比特数（每个元素）可以通过以下公式计算：
//
// $$
// \text{BPE} = \frac{-\ln(\text{error rate})}{(\ln(2))^2}
// $$
//
// 这里：
// - $\ln$是自然对数。
// - `error rate`是期望的错误率（false positive rate）。
//
// 函数`calc_bpe`实现了上述公式。它首先计算错误率的自然对数（`log(error)`），然后用这个值除以$\ln(2)^2$，最后取结果的绝对值（以确保结果为正，因为错误率（`error`）应该是小于1的正数，所以其对数是负的）。
// 
// 这个计算结果告诉我们，为了达到指定的错误率，我们需要为布隆过滤器中的每个元素分配多少比特。这个值是设计布隆过滤器时的关键参数之一，它影响着布隆过滤器的空间效率和准确性。
// 假设计算结果为 bpe = 9.6。这意味着为了达到1%的误报率，
// 我们需要为布隆过滤器中的每个元素分配大约10位。由于位数不能为小数，我们通常会取整数位，即在实际应用中我们会选择10位或向上取整到接近的整数值
// 根据计算出的 bpe 值，如果我们预期要存储1000个元素，那么布隆过滤器的总位数应该是 1000 * 10 = 10000位
func calcBpe(errorRate float64) float64 {
	const denom = 0.480453013918201 // ln(2)^2
	num := math.Log(errorRate)      // 计算错误率的自然对数

	bpe := -(num / denom)
	if bpe < 0 {
		bpe = -bpe
	}
	return bpe
}
