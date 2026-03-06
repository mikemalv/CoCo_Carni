"use client";

import { useState, useEffect } from "react";
import { prefetchAllData } from "@/lib/cache";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Users, Dices, BrainCircuit, FileText } from "lucide-react";
import MemberOverview from "@/components/member-overview";
import SlotAnalytics from "@/components/slot-analytics";
import MLModels from "@/components/ml-models";
import Policies from "@/components/policies";
import { ThemeToggle } from "@/components/theme-toggle";

export default function Home() {
  const [activeTab, setActiveTab] = useState("members");

  useEffect(() => {
    prefetchAllData();
  }, []);

  return (
    <main className="min-h-screen bg-background">
      <div className="border-b bg-card">
        <div className="container mx-auto py-6 px-4">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-4">
              {/* eslint-disable-next-line @next/next/no-img-element */}
              <img 
                src="/carnival-logo.svg" 
                alt="Carnival" 
                width={160} 
                height={40}
                className="dark:brightness-125"
              />
              <div className="h-8 w-px bg-border" />
              <div>
                <h1 className="text-xl font-bold tracking-tight" style={{color: 'var(--carnival-blue)'}}>Casino Slot Analytics</h1>
                <p className="text-xs text-muted-foreground">FUN FOR ALL. ALL FOR FUN.</p>
              </div>
            </div>
            <ThemeToggle />
          </div>
        </div>
      </div>

      <div className="container mx-auto py-6 px-4">
        <Tabs value={activeTab} onValueChange={setActiveTab} className="space-y-6">
          <TabsList className="grid w-full grid-cols-4 lg:w-[600px]">
            <TabsTrigger value="members" className="flex items-center gap-2">
              <Users className="w-4 h-4" />
              <span className="hidden sm:inline">Member Overview</span>
            </TabsTrigger>
            <TabsTrigger value="slots" className="flex items-center gap-2">
              <Dices className="w-4 h-4" />
              <span className="hidden sm:inline">Slot Analytics</span>
            </TabsTrigger>
            <TabsTrigger value="ml" className="flex items-center gap-2">
              <BrainCircuit className="w-4 h-4" />
              <span className="hidden sm:inline">ML Models</span>
            </TabsTrigger>
            <TabsTrigger value="policies" className="flex items-center gap-2">
              <FileText className="w-4 h-4" />
              <span className="hidden sm:inline">Policies</span>
            </TabsTrigger>
          </TabsList>

          <TabsContent value="members" forceMount className={activeTab !== "members" ? "hidden" : ""}>
            <MemberOverview />
          </TabsContent>
          <TabsContent value="slots" forceMount className={activeTab !== "slots" ? "hidden" : ""}>
            <SlotAnalytics />
          </TabsContent>
          <TabsContent value="ml" forceMount className={activeTab !== "ml" ? "hidden" : ""}>
            <MLModels />
          </TabsContent>
          <TabsContent value="policies" forceMount className={activeTab !== "policies" ? "hidden" : ""}>
            <Policies />
          </TabsContent>
        </Tabs>
      </div>
    </main>
  );
}
