import React, { createContext, useContext, useState, useEffect } from "react";
import type { ServiceAccount } from "@/types/coreTypes"; // Your central type definition

interface ServiceAccountContextType {
  accounts: ServiceAccount[];
  createAccount: () => Promise<void>;
  refreshToken: (id: string) => Promise<void>;
  deleteAccount: (id: string) => Promise<void>;
  setAccounts: React.Dispatch<React.SetStateAction<ServiceAccount[]>>;
}

const ServiceAccountContext = createContext<ServiceAccountContextType | undefined>(undefined);

export const ServiceAccountProvider: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const [accounts, setAccounts] = useState<ServiceAccount[]>([]);

  useEffect(() => {
    const fetchAccounts = async () => {
      const res = await fetch("/v1/service-accounts", {
        headers: {
          "Content-Type": "application/json",
          "Email": "joe@burla.dev",
        },
      });
      if (!res.ok) {
        console.error("Failed to fetch service accounts");
        return;
      }
      const data = await res.json();
      setAccounts(data.service_accounts || []);
    };

    fetchAccounts();
  }, []);

  const createAccount = async () => {
    const res = await fetch("/v1/service-accounts", {  
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Email": "joe@burla.dev",
      },
    });
    if (!res.ok) throw new Error("Failed to create service account");
    const data: ServiceAccount = await res.json();
    setAccounts(prev => [...prev, data]);
  };

  const refreshToken = async (id: string) => {
    const res = await fetch(`/v1/service-accounts/${id}/refresh-token`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Email": "joe@burla.dev",
      },
    });
    
    if (!res.ok) throw new Error("Failed to refresh token");
    const data: { token: string } = await res.json();
    setAccounts(prev =>
      prev.map(acc => acc.id === id ? { ...acc, token: data.token } : acc)
    );
  };

  const deleteAccount = async (id: string) => {
    const res = await fetch(`/v1/service-accounts/${id}`, {
      method: "DELETE",
      headers: {
        "Content-Type": "application/json",
        "Email": "joe@burla.dev",
      },
    });
    if (!res.ok) throw new Error("Failed to delete account");
    setAccounts(prev => prev.filter(acc => acc.id !== id));
  };

  return (
    <ServiceAccountContext.Provider
      value={{ accounts, createAccount, refreshToken, deleteAccount, setAccounts }}
    >
      {children}
    </ServiceAccountContext.Provider>
  );
};

export const useServiceAccounts = (): ServiceAccountContextType => {
  const context = useContext(ServiceAccountContext);
  if (!context) {
    throw new Error("useServiceAccounts must be used within a ServiceAccountProvider");
  }
  return context;
};