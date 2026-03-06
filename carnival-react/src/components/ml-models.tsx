"use client";

import { useEffect, useState, useMemo, useRef } from "react";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Separator } from "@/components/ui/separator";
import { ChartContainer, ChartTooltip, ChartTooltipContent } from "@/components/ui/chart";
import { Bar, BarChart, XAxis, YAxis, CartesianGrid } from "recharts";
import { BrainCircuit, Loader2 } from "lucide-react";
import { fetchWithCache } from "@/lib/cache";

interface Model {
  MODEL_NAME: string;
  MODEL_TYPE: string;
  TARGET: string;
  MODEL_ACCURACY_PCT: number;
  BASELINE_ACCURACY_PCT: number;
  NOTES: string;
}

interface Member {
  MEMBER_ID: number;
  GENDER: string;
  AGE: number;
  MEMBERSHIP_TIER: string;
  RISK_APPETITE: string;
  INCOME_BRACKET: string;
}

interface MLData {
  models: Model[];
  confusion: { ACTUAL: string; PREDICTED: string; CNT: number }[];
  features: { FEATURE: string; SCORE: number }[];
  members: Member[];
}

interface PredictionResult {
  denomination: string;
  betCategory: string;
  profile: Record<string, string | number>;
}

export default function MLModels() {
  const [data, setData] = useState<MLData | null>(null);
  const [loading, setLoading] = useState(true);
  const [tierFilter, setTierFilter] = useState("All");
  const [riskFilter, setRiskFilter] = useState("All");
  const [incomeFilter, setIncomeFilter] = useState("All");
  const [selectedMember, setSelectedMember] = useState<number | null>(null);
  const [predicting, setPredicting] = useState(false);
  const [prediction, setPrediction] = useState<PredictionResult | null>(null);

  const fetched = useRef(false);
  useEffect(() => {
    if (fetched.current) return;
    fetched.current = true;
    fetchWithCache<MLData>("/api/ml")
      .then((d) => {
        setData(d);
        if (d.members?.length > 0) setSelectedMember(d.members[0].MEMBER_ID);
      })
      .finally(() => setLoading(false));
  }, []);

  const filteredMembers = useMemo(() => {
    if (!data) return [];
    let result = data.members;
    if (tierFilter !== "All") result = result.filter((m) => m.MEMBERSHIP_TIER === tierFilter);
    if (riskFilter !== "All") result = result.filter((m) => m.RISK_APPETITE === riskFilter);
    if (incomeFilter !== "All") result = result.filter((m) => m.INCOME_BRACKET === incomeFilter);
    return result;
  }, [data, tierFilter, riskFilter, incomeFilter]);

  const tiers = useMemo(() => data ? [...new Set(data.members.map((m) => m.MEMBERSHIP_TIER))].sort() : [], [data]);
  const risks = useMemo(() => data ? [...new Set(data.members.map((m) => m.RISK_APPETITE))].sort() : [], [data]);
  const incomes = useMemo(() => data ? [...new Set(data.members.map((m) => m.INCOME_BRACKET))].sort() : [], [data]);

  const confusionMatrix = useMemo(() => {
    if (!data) return { rows: [] as string[], cols: [] as string[], grid: {} as Record<string, Record<string, number>> };
    const rows = [...new Set(data.confusion.map((c) => c.ACTUAL))].sort();
    const cols = [...new Set(data.confusion.map((c) => c.PREDICTED))].sort();
    const grid: Record<string, Record<string, number>> = {};
    rows.forEach((r) => {
      grid[r] = {};
      cols.forEach((c) => (grid[r][c] = 0));
    });
    data.confusion.forEach((c) => (grid[c.ACTUAL][c.PREDICTED] = c.CNT));
    return { rows, cols, grid };
  }, [data]);

  const sortedFeatures = useMemo(() => {
    if (!data) return [];
    return [...data.features].sort((a, b) => Number(b.SCORE) - Number(a.SCORE)).slice(0, 15);
  }, [data]);

  async function runPredictions() {
    if (!selectedMember) return;
    setPredicting(true);
    setPrediction(null);
    try {
      const res = await fetch("/api/predict", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ memberId: selectedMember }),
      });
      const result = await res.json();
      setPrediction(result);
    } catch (err) {
      console.error("Prediction failed:", err);
    } finally {
      setPredicting(false);
    }
  }

  if (loading) {
    return (
      <div className="space-y-6">
        {[...Array(3)].map((_, i) => (
          <Card key={i}>
            <CardHeader><Skeleton className="h-6 w-48" /></CardHeader>
            <CardContent><Skeleton className="h-[200px] w-full" /></CardContent>
          </Card>
        ))}
      </div>
    );
  }

  if (!data) return null;

  return (
    <div className="space-y-6">
      <div className="space-y-4">
        <h2 className="text-xl font-semibold">ML Model Performance</h2>
        {data.models.map((model) => {
          const improvement = Number((model.MODEL_ACCURACY_PCT - model.BASELINE_ACCURACY_PCT).toFixed(2));
          return (
            <Card key={model.MODEL_NAME}>
              <CardContent className="pt-6">
                <div className="flex items-center justify-between mb-4">
                  <div>
                    <p className="font-semibold text-lg">{model.MODEL_NAME}</p>
                    <p className="text-sm text-muted-foreground">{model.MODEL_TYPE}</p>
                  </div>
                  <Badge variant="secondary">Target: {model.TARGET}</Badge>
                </div>
                <div className="grid grid-cols-3 gap-4">
                  <div>
                    <p className="text-sm text-muted-foreground">Model Accuracy</p>
                    <p className="text-2xl font-bold">{model.MODEL_ACCURACY_PCT}%</p>
                  </div>
                  <div>
                    <p className="text-sm text-muted-foreground">Baseline Accuracy</p>
                    <p className="text-2xl font-bold">{model.BASELINE_ACCURACY_PCT}%</p>
                  </div>
                  <div>
                    <p className="text-sm text-muted-foreground">Improvement</p>
                    <p className={`text-2xl font-bold ${improvement >= 0 ? "text-green-600" : "text-red-500"}`}>
                      {improvement >= 0 ? "+" : ""}{improvement}pp
                    </p>
                  </div>
                </div>
                <p className="text-sm text-muted-foreground mt-3">{model.NOTES}</p>
              </CardContent>
            </Card>
          );
        })}
      </div>

      <Separator />

      <Card>
        <CardHeader>
          <CardTitle>Denomination Prediction Details</CardTitle>
          <CardDescription>Confusion matrix showing actual vs predicted denomination</CardDescription>
        </CardHeader>
        <CardContent className="overflow-x-auto">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead className="font-bold">Actual \ Predicted</TableHead>
                {confusionMatrix.cols.map((c) => (
                  <TableHead key={c} className="text-center">${c}</TableHead>
                ))}
              </TableRow>
            </TableHeader>
            <TableBody>
              {confusionMatrix.rows.map((r) => (
                <TableRow key={r}>
                  <TableCell className="font-medium">${r}</TableCell>
                  {confusionMatrix.cols.map((c) => (
                    <TableCell key={c} className={`text-center ${r === c ? "bg-muted font-bold" : ""}`}>
                      {confusionMatrix.grid[r][c] || ""}
                    </TableCell>
                  ))}
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Feature Importance - Denomination Model</CardTitle>
        </CardHeader>
        <CardContent>
          {sortedFeatures.length > 0 ? (
            <ChartContainer config={{}} className="h-[400px]">
              <BarChart data={sortedFeatures} layout="vertical">
                <CartesianGrid strokeDasharray="3 3" horizontal={false} />
                <XAxis type="number" tickLine={false} axisLine={false} />
                <YAxis dataKey="FEATURE" type="category" tickLine={false} axisLine={false} width={160} fontSize={11} />
                <ChartTooltip content={<ChartTooltipContent />} />
                <Bar dataKey="SCORE" fill="#F59E0B" radius={[0, 4, 4, 0]} />
              </BarChart>
            </ChartContainer>
          ) : (
            <p className="text-sm text-muted-foreground">Could not load feature importance.</p>
          )}
        </CardContent>
      </Card>

      <Separator />

      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <BrainCircuit className="w-5 h-5" />
            Live ML Predictions
          </CardTitle>
          <CardDescription>
            Use the Model Registry models to predict denomination preference and bet category for any member.
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            <div>
              <label className="text-sm font-medium mb-1.5 block">Membership Tier</label>
              <Select value={tierFilter} onValueChange={setTierFilter}>
                <SelectTrigger><SelectValue /></SelectTrigger>
                <SelectContent>
                  <SelectItem value="All">All</SelectItem>
                  {tiers.map((t) => <SelectItem key={t} value={t}>{t}</SelectItem>)}
                </SelectContent>
              </Select>
            </div>
            <div>
              <label className="text-sm font-medium mb-1.5 block">Risk Appetite</label>
              <Select value={riskFilter} onValueChange={setRiskFilter}>
                <SelectTrigger><SelectValue /></SelectTrigger>
                <SelectContent>
                  <SelectItem value="All">All</SelectItem>
                  {risks.map((r) => <SelectItem key={r} value={r}>{r}</SelectItem>)}
                </SelectContent>
              </Select>
            </div>
            <div>
              <label className="text-sm font-medium mb-1.5 block">Income Bracket</label>
              <Select value={incomeFilter} onValueChange={setIncomeFilter}>
                <SelectTrigger><SelectValue /></SelectTrigger>
                <SelectContent>
                  <SelectItem value="All">All</SelectItem>
                  {incomes.map((i) => <SelectItem key={i} value={i}>{i}</SelectItem>)}
                </SelectContent>
              </Select>
            </div>
          </div>

          <p className="text-sm text-muted-foreground">{filteredMembers.length} members match filters</p>

          <div>
            <label className="text-sm font-medium mb-1.5 block">Select a Member</label>
            <Select
              value={selectedMember?.toString() || ""}
              onValueChange={(v) => setSelectedMember(Number(v))}
            >
              <SelectTrigger><SelectValue /></SelectTrigger>
              <SelectContent>
                {filteredMembers.slice(0, 100).map((m) => (
                  <SelectItem key={m.MEMBER_ID} value={m.MEMBER_ID.toString()}>
                    {m.MEMBER_ID} — {m.GENDER}, Age {m.AGE}, {m.MEMBERSHIP_TIER}, {m.RISK_APPETITE} risk, {m.INCOME_BRACKET}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>

          <Button onClick={runPredictions} disabled={!selectedMember || predicting}>
            {predicting && <Loader2 className="w-4 h-4 mr-2 animate-spin" />}
            Run Predictions
          </Button>

          {prediction && (
            <div className="space-y-4 mt-4">
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <Card className="border-green-200 bg-green-50 dark:bg-green-950/20 dark:border-green-900">
                  <CardContent className="pt-4">
                    <p className="text-sm text-muted-foreground mb-1">Denomination Prediction</p>
                    <p className="text-lg font-semibold text-green-700 dark:text-green-400">{prediction.denomination}</p>
                  </CardContent>
                </Card>
                <Card className="border-blue-200 bg-blue-50 dark:bg-blue-950/20 dark:border-blue-900">
                  <CardContent className="pt-4">
                    <p className="text-sm text-muted-foreground mb-1">Bet Category Prediction</p>
                    <p className="text-lg font-semibold text-blue-700 dark:text-blue-400">{prediction.betCategory}</p>
                  </CardContent>
                </Card>
              </div>

              {prediction.profile && (
                <>
                  <p className="text-sm text-muted-foreground">Member Profile</p>
                  <div className="overflow-x-auto">
                    <Table>
                      <TableHeader>
                        <TableRow>
                          {Object.keys(prediction.profile).map((k) => (
                            <TableHead key={k}>{k}</TableHead>
                          ))}
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        <TableRow>
                          {Object.values(prediction.profile).map((v, i) => (
                            <TableCell key={i}>{String(v)}</TableCell>
                          ))}
                        </TableRow>
                      </TableBody>
                    </Table>
                  </div>
                </>
              )}
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
