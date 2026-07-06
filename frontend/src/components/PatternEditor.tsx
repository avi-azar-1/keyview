import { useState } from "react";
import { useStore } from "../store";
import { addPattern, deletePattern, applyPatterns } from "../api/patterns";

export default function PatternEditor() {
  const patterns = useStore((s) => s.patterns);
  const setPatterns = useStore((s) => s.setPatterns);
  const updatePatternCounts = useStore((s) => s.updatePatternCounts);
  const [input, setInput] = useState("");
  const [applying, setApplying] = useState(false);

  async function handleAdd(e: React.FormEvent) {
    e.preventDefault();
    if (!input.trim()) return;
    const pattern = await addPattern(input.trim());
    setPatterns([...patterns, pattern]);
    setInput("");
  }

  async function handleDelete(id: string) {
    await deletePattern(id);
    setPatterns(patterns.filter((p) => p.id !== id));
  }

  async function handleApply() {
    setApplying(true);
    try {
      const result = await applyPatterns();
      updatePatternCounts(result.pattern_counts);
    } finally {
      setApplying(false);
    }
  }

  return (
    <div className="bg-white dark:bg-gray-800 rounded-xl p-6 shadow-sm border border-gray-100 dark:border-gray-700">
      <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-4">
        Pattern Groups
      </h3>
      <form onSubmit={handleAdd} className="flex gap-2 mb-4">
        <input
          type="text"
          value={input}
          onChange={(e) => setInput(e.target.value)}
          placeholder="e.g. user:* or session:*"
          className="flex-1 px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg bg-white dark:bg-gray-700 text-gray-900 dark:text-white text-sm focus:ring-2 focus:ring-blue-500 focus:border-transparent"
        />
        <button
          type="submit"
          className="px-4 py-2 bg-blue-600 hover:bg-blue-700 text-white text-sm font-medium rounded-lg transition-colors"
        >
          Add
        </button>
        <button
          type="button"
          onClick={handleApply}
          disabled={applying || patterns.length === 0}
          className="px-4 py-2 bg-green-600 hover:bg-green-700 disabled:bg-green-400 text-white text-sm font-medium rounded-lg transition-colors"
        >
          {applying ? "Applying..." : "Apply"}
        </button>
      </form>
      {patterns.length > 0 && (
        <div className="flex flex-wrap gap-2">
          {patterns.map((p) => (
            <span
              key={p.id}
              className="inline-flex items-center gap-1.5 px-3 py-1.5 bg-gray-100 dark:bg-gray-700 rounded-full text-sm text-gray-700 dark:text-gray-300"
            >
              <code className="font-mono">{p.pattern}</code>
              <button
                onClick={() => handleDelete(p.id)}
                className="text-gray-400 hover:text-red-500 transition-colors"
              >
                &times;
              </button>
            </span>
          ))}
        </div>
      )}
    </div>
  );
}
