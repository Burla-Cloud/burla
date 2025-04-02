import React, { useEffect, useState } from "react";
import { Card, CardContent } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import {
  Plus,
  Trash2,
  RefreshCw,
  Clipboard,
  Eye,
  EyeOff,
  Check,
} from "lucide-react";
import { useServiceAccounts } from "@/contexts/ServiceAccountContext";

interface ServiceAccountsProps {
  isEditing: boolean;
}

export const ServiceAccounts: React.FC<ServiceAccountsProps> = ({ isEditing }) => {
  const { accounts, createAccount, refreshToken, deleteAccount } = useServiceAccounts();
  const [selectedIds, setSelectedIds] = useState<Set<string>>(new Set());
  const [visibleTokens, setVisibleTokens] = useState<Set<string>>(new Set());
  const [copiedTokenIds, setCopiedTokenIds] = useState<Set<string>>(new Set());
  const [refreshedTokenIds, setRefreshedTokenIds] = useState<Set<string>>(new Set());
  const [isCondensed, setIsCondensed] = useState<boolean>(false);

  useEffect(() => {
    const checkCondensed = () => setIsCondensed(window.innerWidth < 1000);
    checkCondensed();
    window.addEventListener("resize", checkCondensed);
    return () => window.removeEventListener("resize", checkCondensed);
  }, []);

  const handleCheckboxToggle = (id: string) => {
    setSelectedIds(prev => {
      const updated = new Set(prev);
      updated.has(id) ? updated.delete(id) : updated.add(id);
      return updated;
    });
  };

  const toggleTokenVisibility = (id: string) => {
    setVisibleTokens(prev => {
      const updated = new Set(prev);
      updated.has(id) ? updated.delete(id) : updated.add(id);
      return updated;
    });
  };

  const handleCopy = async (token: string, id: string) => {
    await navigator.clipboard.writeText(token);
    setCopiedTokenIds(prev => new Set(prev).add(id));
    setTimeout(() => {
      setCopiedTokenIds(prev => {
        const updated = new Set(prev);
        updated.delete(id);
        return updated;
      });
    }, 1500);
  };

  const handleRefresh = async (id: string) => {
    await refreshToken(id);
    setRefreshedTokenIds(prev => new Set(prev).add(id));
    setTimeout(() => {
      setRefreshedTokenIds(prev => {
        const updated = new Set(prev);
        updated.delete(id);
        return updated;
      });
    }, 1500);
  };

  const handleDelete = (id: string) => {
    deleteAccount(id);
    setSelectedIds(prev => new Set([...prev].filter(i => i !== id)));
    setVisibleTokens(prev => new Set([...prev].filter(i => i !== id)));
    setCopiedTokenIds(prev => new Set([...prev].filter(i => i !== id)));
    setRefreshedTokenIds(prev => new Set([...prev].filter(i => i !== id)));
  };

  const ActionTooltip = ({ label }: { label: string }) => (
    <div className="absolute -top-6 bg-green-100 text-green-700 text-[10px] px-2 py-0.5 rounded shadow">
      {label}
    </div>
  );

  return (
    <Card className="w-full mt-10">
      <CardContent className="space-y-6 pt-6">
        <div className="flex items-center justify-between">
          <h2 className="text-xl font-semibold text-[#3b5a64]">Service Accounts</h2>
          <Button
            variant="secondary"
            onClick={createAccount}
            disabled={!isEditing}
            className="flex items-center gap-2"
          >
            <Plus className="w-4 h-4" />
            Create
          </Button>
        </div>

        {isCondensed ? (
          <div className="space-y-4">
            {accounts.length === 0 ? (
              <div className="text-center text-gray-500">No service accounts</div>
            ) : (
              accounts.map((acc) => (
                <div key={acc.id} className="border rounded p-4 space-y-3 bg-gray-50">
                  <div className="flex items-center justify-between">
                    <div className="text-sm font-medium text-gray-700">{acc.name}</div>
                    <input
                      type="checkbox"
                      className="h-4 w-4 accent-[#3b5a64] disabled:opacity-50"
                      checked={selectedIds.has(acc.id)}
                      onChange={() => handleCheckboxToggle(acc.id)}
                      disabled={!isEditing}
                    />
                  </div>
                  <div className="w-full overflow-x-auto text-xs font-mono bg-white px-2 py-1 rounded">
                    {visibleTokens.has(acc.id) ? (
                      acc.token
                    ) : (
                      <span className="opacity-60 select-none">{"•".repeat(30)}</span>
                    )}
                  </div>
                  <div className="flex items-center gap-4 justify-end">
                    <div className="relative flex flex-col items-center">
                      {copiedTokenIds.has(acc.id) && <ActionTooltip label="Copied!" />}
                      <button onClick={() => handleCopy(acc.token, acc.id)} title="Copy">
                        {copiedTokenIds.has(acc.id) ? (
                          <Check className="w-4 h-4 text-green-600" />
                        ) : (
                          <Clipboard className="w-4 h-4 text-[#3b5a64] hover:text-gray-700" />
                        )}
                      </button>
                    </div>
                    <button onClick={() => toggleTokenVisibility(acc.id)} title="Show/Hide">
                      {visibleTokens.has(acc.id) ? (
                        <EyeOff className="w-4 h-4 text-[#3b5a64] hover:text-gray-700" />
                      ) : (
                        <Eye className="w-4 h-4 text-[#3b5a64] hover:text-gray-700" />
                      )}
                    </button>
                    <div
                      className={`flex gap-3 relative transition-opacity duration-200 ${
                        isEditing && selectedIds.has(acc.id)
                          ? "opacity-100"
                          : "opacity-0 pointer-events-none"
                      }`}
                    >
                      <div className="relative flex flex-col items-center">
                        {refreshedTokenIds.has(acc.id) && <ActionTooltip label="Refreshed!" />}
                        <button onClick={() => handleRefresh(acc.id)} title="Refresh">
                          <RefreshCw className="w-4 h-4 text-[#3b5a64] hover:text-gray-700" />
                        </button>
                      </div>
                      <button onClick={() => handleDelete(acc.id)} title="Delete">
                        <Trash2 className="w-4 h-4 text-red-500 hover:text-red-700" />
                      </button>
                    </div>
                  </div>
                </div>
              ))
            )}
          </div>
        ) : (
          <div>
            <table className="w-full text-sm text-left table-auto">
              {accounts.length > 0 && (
                <thead className="border-b bg-white">
                  <tr>
                    <th className="p-2"></th>
                    <th className="p-2 pl-4 text-sm font-medium text-gray-500">Name</th>
                    <th className="p-2 pl-4 text-sm font-medium text-gray-500">Token</th>
                  </tr>
                </thead>
              )}
              <tbody>
                {accounts.length === 0 ? (
                  <tr>
                    <td colSpan={3} className="py-4 text-center text-gray-500">
                      No service accounts
                    </td>
                  </tr>
                ) : (
                  accounts.map((acc) => (
                    <tr key={acc.id} className="border-b hover:bg-gray-50">
                      <td className="p-2 align-middle">
                        <input
                          type="checkbox"
                          className="h-4 w-4 accent-[#3b5a64] disabled:opacity-50"
                          checked={selectedIds.has(acc.id)}
                          onChange={() => handleCheckboxToggle(acc.id)}
                          disabled={!isEditing}
                        />
                      </td>
                      <td className="p-2 pl-4 align-middle text-sm text-gray-700">{acc.name}</td>
                      <td className="p-2 pl-4 align-middle">
                        <div className="flex items-center">
                          <div
                            className="w-[350px] flex-shrink-0 px-2 py-1 rounded text-xs font-mono bg-gray-100"
                            style={{ overflowX: "auto", whiteSpace: "nowrap", scrollbarWidth: "none" }}
                          >
                            <div className="select-text" style={{ display: "inline-block", minWidth: "100%" }}>
                              {visibleTokens.has(acc.id) ? (
                                acc.token
                              ) : (
                                <span className="opacity-60 select-none block">{"•".repeat(100)}</span>
                              )}
                            </div>
                          </div>
                          <div className="flex items-center gap-3 ml-[20px] relative">
                            <div className="relative flex flex-col items-center">
                              {copiedTokenIds.has(acc.id) && <ActionTooltip label="Copied!" />}
                              <button onClick={() => handleCopy(acc.token, acc.id)} title="Copy">
                                {copiedTokenIds.has(acc.id) ? (
                                  <Check className="w-4 h-4 text-green-600" />
                                ) : (
                                  <Clipboard className="w-4 h-4 text-[#3b5a64] hover:text-gray-700" />
                                )}
                              </button>
                            </div>
                            <button onClick={() => toggleTokenVisibility(acc.id)} title="Show/Hide">
                              {visibleTokens.has(acc.id) ? (
                                <EyeOff className="w-4 h-4 text-[#3b5a64] hover:text-gray-700" />
                              ) : (
                                <Eye className="w-4 h-4 text-[#3b5a64] hover:text-gray-700" />
                              )}
                            </button>
                            <div
                              className={`flex gap-3 relative transition-opacity duration-200 ${
                                isEditing && selectedIds.has(acc.id)
                                  ? "opacity-100"
                                  : "opacity-0 pointer-events-none"
                              }`}
                            >
                              <div className="relative flex flex-col items-center">
                                {refreshedTokenIds.has(acc.id) && <ActionTooltip label="Refreshed!" />}
                                <button onClick={() => handleRefresh(acc.id)} title="Refresh">
                                  <RefreshCw className="w-4 h-4 text-[#3b5a64] hover:text-gray-700" />
                                </button>
                              </div>
                              <button onClick={() => handleDelete(acc.id)} title="Delete">
                                <Trash2 className="w-4 h-4 text-red-500 hover:text-red-700" />
                              </button>
                            </div>
                          </div>
                        </div>
                      </td>
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          </div>
        )}
      </CardContent>
    </Card>
  );
};