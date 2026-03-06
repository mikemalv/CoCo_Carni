"use client";

import { useEffect, useState, useRef } from "react";
import { Card, CardContent } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { Badge } from "@/components/ui/badge";
import { ChevronDown, ChevronRight } from "lucide-react";
import { fetchWithCache } from "@/lib/cache";

interface Policy {
  POLICY_ID: number;
  CATEGORY: string;
  TITLE: string;
  CONTENT: string;
  LAST_UPDATED: string;
}

export default function Policies() {
  const [policies, setPolicies] = useState<Policy[]>([]);
  const [loading, setLoading] = useState(true);
  const [expanded, setExpanded] = useState<Set<number>>(new Set());

  const fetched = useRef(false);
  useEffect(() => {
    if (fetched.current) return;
    fetched.current = true;
    fetchWithCache<Policy[]>("/api/policies")
      .then(setPolicies)
      .finally(() => setLoading(false));
  }, []);

  function toggle(id: number) {
    setExpanded((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  }

  if (loading) {
    return (
      <div className="space-y-6">
        {[...Array(3)].map((_, i) => (
          <div key={i} className="space-y-3">
            <Skeleton className="h-6 w-32" />
            <Skeleton className="h-16 w-full" />
            <Skeleton className="h-16 w-full" />
          </div>
        ))}
      </div>
    );
  }

  const categories = [...new Set(policies.map((p) => p.CATEGORY))];

  return (
    <div className="space-y-8">
      <h2 className="text-xl font-semibold">Casino Policies & Information</h2>
      {categories.map((cat) => (
        <div key={cat} className="space-y-3">
          <h3 className="text-lg font-semibold">{cat}</h3>
          {policies
            .filter((p) => p.CATEGORY === cat)
            .map((policy) => {
              const isOpen = expanded.has(policy.POLICY_ID);
              return (
                <Card key={policy.POLICY_ID} className="cursor-pointer" onClick={() => toggle(policy.POLICY_ID)}>
                  <CardContent className="pt-4 pb-4">
                    <div className="flex items-center justify-between">
                      <div className="flex items-center gap-2">
                        {isOpen ? <ChevronDown className="w-4 h-4" /> : <ChevronRight className="w-4 h-4" />}
                        <span className="font-medium">{policy.TITLE}</span>
                      </div>
                      <Badge variant="outline" className="text-xs">
                        Updated: {policy.LAST_UPDATED}
                      </Badge>
                    </div>
                    {isOpen && (
                      <div className="mt-3 pl-6 text-sm text-muted-foreground whitespace-pre-wrap">
                        {policy.CONTENT}
                      </div>
                    )}
                  </CardContent>
                </Card>
              );
            })}
        </div>
      ))}
    </div>
  );
}
