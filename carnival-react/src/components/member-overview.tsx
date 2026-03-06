"use client";

import { useEffect, useState, useRef } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { ChartContainer, ChartTooltip, ChartTooltipContent } from "@/components/ui/chart";
import { Bar, BarChart, XAxis, YAxis, CartesianGrid, Cell } from "recharts";
import { Users, Play, DollarSign, Activity } from "lucide-react";
import { fetchWithCache } from "@/lib/cache";

interface MemberData {
  kpis: {
    total_members: number;
    total_sessions: number;
    total_wagered: number;
    avg_sessions: number;
  };
  tiers: { MEMBERSHIP_TIER: string; MEMBERS: number }[];
  ages: { AGE_GROUP: string; GENDER: string; CNT: number }[];
  income: { INCOME_BRACKET: string; CNT: number }[];
  states: { HOME_STATE: string; MEMBERS: number }[];
}

const tierColors: Record<string, string> = {
  Platinum: "#7C3AED",
  Gold: "#F59E0B",
  Silver: "#94A3B8",
  Bronze: "#B45309",
  Basic: "#64748B",
};

export default function MemberOverview() {
  const [data, setData] = useState<MemberData | null>(null);
  const [loading, setLoading] = useState(true);

  const fetched = useRef(false);
  useEffect(() => {
    if (fetched.current) return;
    fetched.current = true;
    fetchWithCache<MemberData>("/api/members")
      .then(setData)
      .finally(() => setLoading(false));
  }, []);

  if (loading) {
    return (
      <div className="space-y-6">
        <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
          {[...Array(4)].map((_, i) => (
            <Card key={i}>
              <CardContent className="pt-6">
                <Skeleton className="h-4 w-20 mb-2" />
                <Skeleton className="h-8 w-32" />
              </CardContent>
            </Card>
          ))}
        </div>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
          {[...Array(4)].map((_, i) => (
            <Card key={i}>
              <CardHeader>
                <Skeleton className="h-6 w-40" />
              </CardHeader>
              <CardContent>
                <Skeleton className="h-[300px] w-full" />
              </CardContent>
            </Card>
          ))}
        </div>
      </div>
    );
  }

  if (!data) return null;

  const tiersWithColors = data.tiers.map((t) => ({
    ...t,
    fill: tierColors[t.MEMBERSHIP_TIER] || "#6B7280",
  }));

  const maleAges = data.ages.filter((a) => a.GENDER === "M");
  const femaleAges = data.ages.filter((a) => a.GENDER === "F");
  const ageGroups = [...new Set(data.ages.map((a) => a.AGE_GROUP))].sort();
  const ageData = ageGroups.map((group) => ({
    ageGroup: group,
    Male: maleAges.find((a) => a.AGE_GROUP === group)?.CNT || 0,
    Female: femaleAges.find((a) => a.AGE_GROUP === group)?.CNT || 0,
  }));

  return (
    <div className="space-y-6">
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        <Card>
          <CardContent className="pt-6">
            <div className="flex items-center gap-2 text-muted-foreground text-sm">
              <Users className="w-4 h-4" />
              Total Members
            </div>
            <p className="text-3xl font-bold mt-1">{Math.round(Number(data.kpis.total_members)).toLocaleString()}</p>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="pt-6">
            <div className="flex items-center gap-2 text-muted-foreground text-sm">
              <Play className="w-4 h-4" />
              Total Play Sessions
            </div>
            <p className="text-3xl font-bold mt-1">{Math.round(Number(data.kpis.total_sessions)).toLocaleString()}</p>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="pt-6">
            <div className="flex items-center gap-2 text-muted-foreground text-sm">
              <DollarSign className="w-4 h-4" />
              Total Wagered
            </div>
            <p className="text-3xl font-bold mt-1">${Math.round(Number(data.kpis.total_wagered)).toLocaleString()}</p>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="pt-6">
            <div className="flex items-center gap-2 text-muted-foreground text-sm">
              <Activity className="w-4 h-4" />
              Avg Sessions/Member
            </div>
            <p className="text-3xl font-bold mt-1">{Math.round(Number(data.kpis.avg_sessions))}</p>
          </CardContent>
        </Card>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        <Card>
          <CardHeader>
            <CardTitle>Membership Tier Distribution</CardTitle>
          </CardHeader>
          <CardContent>
            <ChartContainer config={{}} className="h-[300px]">
              <BarChart data={tiersWithColors} layout="horizontal">
                <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="hsl(var(--border))" />
                <XAxis dataKey="MEMBERSHIP_TIER" tickLine={false} axisLine={false} />
                <YAxis tickLine={false} axisLine={false} />
                <ChartTooltip content={<ChartTooltipContent />} />
                <Bar dataKey="MEMBERS" radius={[6, 6, 0, 0]}>
                  {tiersWithColors.map((entry: { MEMBERSHIP_TIER: string; fill: string }, index: number) => (
                    <Cell key={index} fill={entry.fill} fillOpacity={0.85} />
                  ))}
                </Bar>
              </BarChart>
            </ChartContainer>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Age Distribution by Gender</CardTitle>
          </CardHeader>
          <CardContent>
            <ChartContainer config={{ Male: { color: "#014E8F" }, Female: { color: "#B61B38" } }} className="h-[300px]">
              <BarChart data={ageData}>
                <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="hsl(var(--border))" />
                <XAxis dataKey="ageGroup" tickLine={false} axisLine={false} />
                <YAxis tickLine={false} axisLine={false} />
                <ChartTooltip content={<ChartTooltipContent />} />
                <Bar dataKey="Male" fill="#014E8F" radius={[6, 6, 0, 0]} fillOpacity={0.85} />
                <Bar dataKey="Female" fill="#B61B38" radius={[6, 6, 0, 0]} fillOpacity={0.85} />
              </BarChart>
            </ChartContainer>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Income Bracket Distribution</CardTitle>
          </CardHeader>
          <CardContent>
            <ChartContainer config={{ CNT: { label: "Members", color: "var(--carnival-blue)" } }} className="h-[250px]">
              <BarChart data={data.income}>
                <defs>
                  <linearGradient id="incomeGradient" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="0%" stopColor="var(--carnival-blue)" stopOpacity={0.9} />
                    <stop offset="100%" stopColor="var(--carnival-blue)" stopOpacity={0.5} />
                  </linearGradient>
                </defs>
                <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="hsl(var(--border))" />
                <XAxis dataKey="INCOME_BRACKET" tickLine={false} axisLine={false} fontSize={12} />
                <YAxis tickLine={false} axisLine={false} />
                <ChartTooltip content={<ChartTooltipContent />} />
                <Bar dataKey="CNT" fill="url(#incomeGradient)" radius={[6, 6, 0, 0]} />
              </BarChart>
            </ChartContainer>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Home State Distribution</CardTitle>
          </CardHeader>
          <CardContent>
            <ChartContainer config={{ MEMBERS: { label: "Members", color: "var(--carnival-red)" } }} className="h-[250px]">
              <BarChart data={data.states.slice(0, 10)}>
                <defs>
                  <linearGradient id="stateGradient" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="0%" stopColor="var(--carnival-red)" stopOpacity={0.9} />
                    <stop offset="100%" stopColor="var(--carnival-red)" stopOpacity={0.5} />
                  </linearGradient>
                </defs>
                <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="hsl(var(--border))" />
                <XAxis dataKey="HOME_STATE" tickLine={false} axisLine={false} />
                <YAxis tickLine={false} axisLine={false} />
                <ChartTooltip content={<ChartTooltipContent />} />
                <Bar dataKey="MEMBERS" fill="url(#stateGradient)" radius={[6, 6, 0, 0]} />
              </BarChart>
            </ChartContainer>
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
