$ErrorActionPreference = "Stop"

$path = "D:\llumnix-ray-main\results\full_data_export_20260824_151951\Llumnix_完整实验数据统计表.html"
$backup = "$path.bak_before_content_update_20260824"

if (-not (Test-Path -LiteralPath $backup)) {
    Copy-Item -LiteralPath $path -Destination $backup
}

$html = Get-Content -LiteralPath $path -Raw -Encoding UTF8
$options = [System.Text.RegularExpressions.RegexOptions]::Singleline

$configRows = @(
    @("实例数量", "4 个 vLLM 实例，对应 4 张 RTX 5090"),
    @("模型", "Qwen2.5-7B-Instruct"),
    @("50 请求实验", "每轮 50 请求，并发约 20，重复 3 轮"),
    @("500 请求实验", "每轮 500 请求，并发 50，重复 3 轮"),
    @("稳定短请求", "短输入，<code>max_tokens=32</code>"),
    @("混合长尾", "80% 输出 48 tokens，20% 输出 192 tokens"),
    @("长输出迁移", "短输入，<code>max_tokens=256</code>"),
    @("请求到达模式", "并发 worker 持续提交，属于受控并发，不是固定 QPS/Poisson"),
    @("采样参数", "<code>temperature=0.0</code>，<code>ignore_eos=True</code>"),
    @("请求公平性", "相同 workload 使用相同请求文件、seed 和 prompt"),
    @("调度策略", "Hybrid 使用 <code>flood + full mode</code>；另有 Round Robin、Load Balance"),
    @("迁移策略", "Random、Load-FCR、Load-SR、Remaining-longest、TPOT、Proposed"),
    @("迁移后端", "HybridConnector + vllm-kvt，单机地址 <code>127.0.0.1</code>"),
    @("GPU 内存利用率", "当前多数实验为 <code>0.50</code>")
)
$configBody = ($configRows | ForEach-Object { "<tr><td>$($_[0])</td><td>$($_[1])</td></tr>" }) -join ""
$configSection = "<section><h2>实验配置</h2><div class=`"table-wrap`"><table><thead><tr><th>配置</th><th>当前设置</th></tr></thead><tbody>$configBody</tbody></table></div></section>"
$html = [regex]::Replace($html, "<section><h2>(?:数据清单|实验配置及作用|实验配置)</h2>.*?</section>", $configSection, $options)

function Remove-Section([string]$content, [string]$heading) {
    $escaped = [regex]::Escape($heading)
    return [regex]::Replace($content, "<section><h2>$escaped</h2>.*?</section>", "", $options)
}

function Add-Section-Note([string]$content, [string]$heading, [string]$marker, [string]$note) {
    $escaped = [regex]::Escape($heading)
    $pattern = "(<section><h2>$escaped</h2>.*?)(</section>)"
    return [regex]::Replace(
        $content,
        $pattern,
        {
            param($match)
            if ($match.Groups[1].Value.Contains("data-note=`"$marker`"")) {
                return $match.Value
            }
            return $match.Groups[1].Value + "<div class=`"tagline note-block`" data-note=`"$marker`">$note</div>" + $match.Groups[2].Value
        },
        $options
    )
}

foreach ($heading in @(
    "四实例正式实验（三轮明细）",
    "500 请求多基线：迁移与异常",
    "Figure 10：迁移效率最小压力复跑",
    "数据集异常计数",
    "无效或归档轮次"
)) {
    $html = Remove-Section $html $heading
}

$html = $html.Replace("<h2>公平基线（按有效轮次均值）</h2>", "<h2>50请求实验</h2>")

$note50 = "增加了三类固定负载：稳定短请求、混合长尾、长输出迁移，50 个请求跑了三轮。稳定短请求：每个请求 <code>max_tokens=32</code>；混合长尾：80% 为 48 tokens，20% 为 192 tokens；长输出迁移：每个请求 <code>max_tokens=256</code>。迁移没有解决足够严重的负载问题，反而有少数请求承担了迁移本身的额外开销，于是 P99 被这些请求拉高。尤其是稳定短请求，开始迁移时请求可能已经接近完成，此时搬运一次 KV Cache 的成本，在总时延只有半秒左右的请求上会非常显眼。"
$html = Add-Section-Note $html "50请求实验" "fair-50" $note50

$note500 = "Hybrid 实时迁移与 Hybrid 不迁移的整体性能均明显低于 Load Balance 和 Round Robin baseline，怀疑完整调度路径本身开销较大。相比 50 请求，500 请求下迁移对短请求的改善更明显，说明短请求并不是天然不适合迁移。对单个短请求而言，迁移额外时延占总时延的比例更明显，迁移过于频繁容易得不偿失；但短请求的 KV 较小，迁移成本也相对较低。"
$html = Add-Section-Note $html "500 请求多基线：性能" "multi-500" $note500

$policyIntro = "<strong>策略简介：</strong>Proposed 综合考虑负载差、剩余长度和 KV 迁移成本；负载 + FCR 按负载差触发并使用默认请求选择规则；负载 + SR 优先迁移当前上下文较短、KV 成本较低的请求；随机迁移从候选请求中随机选择；剩余长度优先选择预计仍需运行较久的请求；TPOT 尾延迟感知优先处理预测每 token 时延或 SLO 风险较高的请求。<br><strong>结果：</strong>长输出下各项指标差别不大，TPOT 尾延迟感知略好；混合长尾下吞吐最好的是 Proposed，P99 最好的是负载 + SR，剩余长度优先策略的尾延迟很高，说明单纯按预计剩余长度选择请求，在混合长尾 workload 下并不是好策略；稳定短请求下 TPOT 表现最好。"
$html = Add-Section-Note $html "54 轮迁移策略：性能" "policy-performance" $policyIntro

$migrationNote = "Proposed 和 TPOT 比较克制，触发迁移较少；结合上一张性能表，两者性能并没有严重劣化，说明迁移并不是越频繁越好。尤其是剩余长度优先策略迁移次数很多、成功率很高，但整体性能较差。"
$html = Add-Section-Note $html "54 轮迁移策略：迁移结果" "policy-migration" $migrationNote

$fig12v1 = "实验目的是验证请求迁移能否整合分散在多个实例上的 KV Cache 空间，缓解内存碎片和队首阻塞，从而降低长输入请求的 TTFT 与尾延迟。目前 v1 没有成功触发目标碎片化现象，因此该版本不能形成正式结论。"
$html = Add-Section-Note $html "Figure 12 v1：碎片化实验" "fig12-v1" $fig12v1

$fig12v2 = "压力窗口扫描的目标是先找到一个既能稳定制造碎片化、又不会把服务压垮的 workload，再进行正式四策略对比。当前已经通过扫描选出 Config C，但高并发迁移时仍偶发 KVT RPC 失败，导致个别请求以 <code>load fail</code> 中止，正式实验尚未完全收口。"
$html = Add-Section-Note $html "Figure 12 v2：压力窗口扫描" "fig12-v2" $fig12v2

if (-not $html.Contains("data-note=`"next-steps`"")) {
    $next = "<section data-note=`"next-steps`"><h2>接下来</h2><div class=`"note-block next-steps-list`">1. 完成 Figure 12<br>2. 进行优先级隔离实验<br>3. 对前面的实验继续增加压力负载，制造不同场景<br>4. 改进 Proposed</div></section>"
    $html = $html.Replace("<footer>", "$next<footer>")
}
$html = $html.Replace("<div class=`"tagline note-block`">1. 完成 Figure 12", "<div class=`"note-block next-steps-list`">1. 完成 Figure 12")

if (-not $html.Contains(".note-block{")) {
    $html = $html.Replace("</style>", ".note-block{margin:8px 0 0;line-height:1.65} .note-block code{font-size:inherit}</style>")
}
if (-not $html.Contains(".next-steps-list{")) {
    $html = $html.Replace("</style>", ".next-steps-list{color:var(--ink);font-size:13px;line-height:1.8}@media(max-width:900px){.next-steps-list{font-size:10px}}</style>")
}

[System.IO.File]::WriteAllText($path, $html, [System.Text.UTF8Encoding]::new($false))
Write-Output "UPDATED=$path"
Write-Output "BACKUP=$backup"
