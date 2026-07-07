import { useStore } from "../store";
import { addPattern } from "../api/patterns";

export default function PrefixSuggestions() {
  const result = useStore((s) => s.scanResult);
  const patterns = useStore((s) => s.patterns);
  const setPatterns = useStore((s) => s.setPatterns);

  if (!result || result.suggested_prefixes.length === 0) {
    return null;
  }

  async function handleAdd(prefix: string) {
    const glob = prefix + "*";
    if (patterns.some((p) => p.pattern === glob)) return;
    const pattern = await addPattern(glob);
    setPatterns([...patterns, pattern]);
  }

  return (
    <div className="bg-white dark:bg-gray-800 rounded-xl p-6 shadow-sm border border-gray-100 dark:border-gray-700">
      <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-4">
        Suggested Prefixes
      </h3>
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="text-left text-gray-500 dark:text-gray-400 border-b border-gray-200 dark:border-gray-700">
              <th className="pb-2 font-medium">Prefix</th>
              <th className="pb-2 font-medium text-right">Keys</th>
              <th className="pb-2 font-medium text-right">Coverage</th>
              <th className="pb-2 font-medium text-right">Branches</th>
              <th className="pb-2 w-16"></th>
            </tr>
          </thead>
          <tbody>
            {result.suggested_prefixes.map((s) => (
              <tr
                key={s.prefix}
                className="border-b border-gray-100 dark:border-gray-700/50 last:border-0"
              >
                <td className="py-2 font-mono text-gray-900 dark:text-white">
                  {s.prefix}
                </td>
                <td className="py-2 text-right text-gray-700 dark:text-gray-300">
                  {s.key_count.toLocaleString()}
                </td>
                <td className="py-2 text-right text-gray-700 dark:text-gray-300">
                  {s.coverage_pct}%
                </td>
                <td className="py-2 text-right text-gray-700 dark:text-gray-300">
                  {s.child_count}
                </td>
                <td className="py-2 text-right">
                  <button
                    onClick={() => handleAdd(s.prefix)}
                    disabled={patterns.some((p) => p.pattern === s.prefix + "*")}
                    className="px-2 py-1 text-xs font-medium bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400 rounded hover:bg-green-200 dark:hover:bg-green-900/50 disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
                  >
                    + Add
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
