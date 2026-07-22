import { useStore } from "../../store";

export default function NamespaceTabs() {
  const result = useStore((s) => s.scanResult);
  const selectedNamespace = useStore((s) => s.selectedNamespace);
  const setSelectedNamespace = useStore((s) => s.setSelectedNamespace);

  const breakdowns = result?.namespace_breakdowns ?? [];
  if (breakdowns.length === 0) return null;

  const allTotal = result?.total_keys ?? 0;
  const tabs = [
    { namespace: "All", total: allTotal },
    ...breakdowns.map((b) => ({ namespace: b.namespace, total: b.total })),
  ];

  return (
    <div className="flex flex-wrap gap-2 mb-4">
      {tabs.map((tab) => {
        const active = tab.namespace === selectedNamespace;
        return (
          <button
            key={tab.namespace}
            onClick={() => setSelectedNamespace(tab.namespace)}
            className={
              "px-3 py-1.5 text-xs font-medium rounded-full transition-colors " +
              (active
                ? "bg-purple-600 text-white"
                : "bg-gray-100 dark:bg-gray-700 text-gray-700 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-gray-600")
            }
          >
            {tab.namespace}
            <span
              className={
                "ml-1.5 " +
                (active ? "text-purple-200" : "text-gray-400 dark:text-gray-500")
              }
            >
              {tab.total.toLocaleString()}
            </span>
          </button>
        );
      })}
    </div>
  );
}
