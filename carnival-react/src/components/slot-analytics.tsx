"use client";

import { useEffect, useState, useRef } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { ChartContainer, ChartTooltip, ChartTooltipContent } from "@/components/ui/chart";
import { Bar, BarChart, XAxis, YAxis, CartesianGrid, Line, LineChart } from "recharts";
import { Users, Coins, Clock, Percent } from "lucide-react";
import { fetchWithCache } from "@/lib/cache";

interface SlotData {
  ships: string[];
  stats: { UNIQUE_PLAYERS: number; AVG_BET: number; AVG_DURATION: number; WIN_RATE: number };
  denominations: { DENOMINATION: number; TOTAL_WAGERED: number; SESSIONS: number }[];
  games: { GAME_NAME: string; SESSIONS: number }[];
  timeOfDay: { TIME_OF_DAY: string; AVG_BET: number; SESSIONS: number }[];
  gameTypes: { GAME_TYPE: string; SESSIONS: number; AVG_BET: number }[];
  monthly: { MONTH: string; TOTAL_WAGERED: number; UNIQUE_PLAYERS: number }[];
}

export default function SlotAnalytics() {
  const [data, setData] = useState<SlotData | null>(null);
  const [loading, setLoading] = useState(true);
  const [ship, setShip] = useState("All Ships");
  const fetchedShips = useRef(new Set<string>());

  useEffect(() => {
    const url = `/api/slots?ship=${encodeURIComponent(ship)}`;
    if (fetchedShips.current.has(ship) && data) {
      setLoading(true);
      fetchWithCache<SlotData>(url).then(setData).finally(() => setLoading(false));
      return;
    }
    setLoading(true);
    fetchWithCache<SlotData>(url)
      .then((d) => {
        setData(d);
        fetchedShips.current.add(ship);
      })
      .finally(() => setLoading(false));
  }, [ship]);

  if (loading && !data) {
    return (
      <div className="space-y-6">
        <Skeleton className="h-10 w-48" />
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
      </div>
    );
  }

  if (!data) return null;

  const denomData = data.denominations.map((d) => ({
    ...d,
    label: `$${d.DENOMINATION}`,
  }));

  return (
    <div className="space-y-6">
      <Card>
        <CardHeader>
          <CardTitle>Slot Play Analysis</CardTitle>
        </CardHeader>
        <CardContent>
          <Select value={ship} onValueChange={setShip}>
            <SelectTrigger className="w-[200px]">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="All Ships">All Ships</SelectItem>
              {data.ships.map((s) => (
                <SelectItem key={s} value={s}>{s}</SelectItem>
              ))}
            </SelectContent>
          </Select>
        </CardContent>
      </Card>

      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        <Card>
          <CardContent className="pt-6">
            <div className="flex items-center gap-2 text-muted-foreground text-sm">
              <Users className="w-4 h-4" />
              Unique Players
            </div>
            <p className="text-3xl font-bold mt-1">{data.stats.UNIQUE_PLAYERS.toLocaleString()}</p>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="pt-6">
            <div className="flex items-center gap-2 text-muted-foreground text-sm">
              <Coins className="w-4 h-4" />
              Avg Bet/Spin
            </div>
            <p className="text-3xl font-bold mt-1">${data.stats.AVG_BET}</p>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="pt-6">
            <div className="flex items-center gap-2 text-muted-foreground text-sm">
              <Clock className="w-4 h-4" />
              Avg Session (min)
            </div>
            <p className="text-3xl font-bold mt-1">{data.stats.AVG_DURATION}</p>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="pt-6">
            <div className="flex items-center gap-2 text-muted-foreground text-sm">
              <Percent className="w-4 h-4" />
              Win Rate
            </div>
            <p className="text-3xl font-bold mt-1">{data.stats.WIN_RATE}%</p>
          </CardContent>
        </Card>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        <Card>
          <CardHeader>
            <CardTitle>Revenue by Denomination</CardTitle>
          </CardHeader>
          <CardContent>
            <ChartContainer config={{ TOTAL_WAGERED: { label: "Total Wagered", color: "#014E8F" } }} className="h-[300px]">
              <BarChart data={denomData}>
                <defs>
                  <linearGradient id="denomGradient" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="0%" stopColor="#014E8F" stopOpacity={0.9} />
                    <stop offset="100%" stopColor="#014E8F" stopOpacity={0.5} />
                  </linearGradient>
                </defs>
                <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="hsl(var(--border))" />
                <XAxis dataKey="label" tickLine={false} axisLine={false} />
                <YAxis tickLine={false} axisLine={false} tickFormatter={(v) => `${(v / 1000).toFixed(0)}k`} />
                <ChartTooltip content={<ChartTooltipContent />} />
                <Bar dataKey="TOTAL_WAGERED" fill="url(#denomGradient)" radius={[6, 6, 0, 0]} />
              </BarChart>
            </ChartContainer>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Top Games by Sessions</CardTitle>
          </CardHeader>
          <CardContent>
            <ChartContainer config={{ SESSIONS: { label: "Sessions", color: "#B61B38" } }} className="h-[300px]">
              <BarChart data={data.games} layout="vertical">
                <defs>
                  <linearGradient id="sessionsGradient" x1="0" y1="0" x2="1" y2="0">
                    <stop offset="0%" stopColor="#B61B38" stopOpacity={0.5} />
                    <stop offset="100%" stopColor="#B61B38" stopOpacity={0.9} />
                  </linearGradient>
                </defs>
                <CartesianGrid strokeDasharray="3 3" horizontal={false} stroke="hsl(var(--border))" />
                <XAxis type="number" tickLine={false} axisLine={false} />
                <YAxis dataKey="GAME_NAME" type="category" tickLine={false} axisLine={false} width={120} fontSize={11} />
                <ChartTooltip content={<ChartTooltipContent />} />
                <Bar dataKey="SESSIONS" fill="url(#sessionsGradient)" radius={[0, 6, 6, 0]} />
              </BarChart>
            </ChartContainer>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Play Patterns by Time of Day</CardTitle>
          </CardHeader>
          <CardContent>
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Time of Day</TableHead>
                  <TableHead className="text-right">Avg Bet</TableHead>
                  <TableHead className="text-right">Sessions</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {data.timeOfDay.map((t) => (
                  <TableRow key={t.TIME_OF_DAY}>
                    <TableCell className="font-medium">{t.TIME_OF_DAY}</TableCell>
                    <TableCell className="text-right">${t.AVG_BET}</TableCell>
                    <TableCell className="text-right">{t.SESSIONS.toLocaleString()}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Game Type Distribution</CardTitle>
          </CardHeader>
          <CardContent>
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Game Type</TableHead>
                  <TableHead className="text-right">Sessions</TableHead>
                  <TableHead className="text-right">Avg Bet</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {data.gameTypes.map((g) => (
                  <TableRow key={g.GAME_TYPE}>
                    <TableCell className="font-medium">{g.GAME_TYPE}</TableCell>
                    <TableCell className="text-right">{g.SESSIONS.toLocaleString()}</TableCell>
                    <TableCell className="text-right">${g.AVG_BET}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </CardContent>
        </Card>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Monthly Wagering Trend</CardTitle>
        </CardHeader>
        <CardContent>
          <ChartContainer config={{ TOTAL_WAGERED: { label: "Total Wagered", color: "#014E8F" } }} className="h-[250px]">
            <LineChart data={data.monthly}>
              <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="hsl(var(--border))" />
              <XAxis dataKey="MONTH" tickLine={false} axisLine={false} />
              <YAxis tickLine={false} axisLine={false} tickFormatter={(v) => `${(v / 1000000).toFixed(1)}M`} />
              <ChartTooltip content={<ChartTooltipContent />} />
              <Line type="monotone" dataKey="TOTAL_WAGERED" stroke="#014E8F" strokeWidth={2.5} dot={false} />
            </LineChart>
          </ChartContainer>
        </CardContent>
      </Card>
    </div>
  );
}
