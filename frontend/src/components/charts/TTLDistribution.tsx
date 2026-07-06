import ReactECharts from "echarts-for-react";
import { useStore } from "../../store";

export default function TTLDistribution() {
  const result = useStore((s) => s.scanResult);
  const darkMode = useStore((s) => s.darkMode);

  if (!result) {
    return (
      <div className="bg-white dark:bg-gray-800 rounded-xl p-6 shadow-sm border border-gray-100 dark:border-gray-700 flex items-center justify-center h-80">
        <span className="text-gray-400">Waiting for scan...</span>
      </div>
    );
  }

  const labels = result.ttl_buckets.map((b) => b.label);
  const values = result.ttl_buckets.map((b) => b.count);

  const option = {
    title: {
      text: "TTL Distribution",
      left: "center",
      textStyle: { color: darkMode ? "#f3f4f6" : "#111827", fontSize: 14 },
    },
    tooltip: { trigger: "axis" },
    xAxis: {
      type: "category",
      data: labels,
      axisLabel: { color: darkMode ? "#9ca3af" : "#6b7280", rotate: 20 },
      axisLine: { lineStyle: { color: darkMode ? "#4b5563" : "#d1d5db" } },
    },
    yAxis: {
      type: "value",
      axisLabel: { color: darkMode ? "#9ca3af" : "#6b7280" },
      splitLine: { lineStyle: { color: darkMode ? "#374151" : "#f3f4f6" } },
    },
    series: [
      {
        type: "bar",
        data: values,
        itemStyle: {
          color: "#8b5cf6",
          borderRadius: [4, 4, 0, 0],
        },
      },
    ],
    grid: { top: 50, bottom: 40, left: 60, right: 20 },
  };

  return (
    <div className="bg-white dark:bg-gray-800 rounded-xl p-6 shadow-sm border border-gray-100 dark:border-gray-700">
      <ReactECharts option={option} style={{ height: 300 }} />
    </div>
  );
}
