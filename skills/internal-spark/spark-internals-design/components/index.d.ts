import type * as React from 'react';

/** Token names usable wherever a prop takes a color: any color token from tokens.json, e.g. 'green', 'key-3', 'node'. */
export type ColorToken = string;

/** The canvas drawing kit returned by `SparkInternals.kit(ctx)`: the episode's own primitives, bound to one 2D context. */
export interface Kit {
  ctx: CanvasRenderingContext2D;
  /** Resolved token colors read from tokens.css. */
  C: Record<string, string>;
  F(weight: number, size: number): string;
  SERIF(weight: number, size: number, italic?: boolean): string;
  color(name: ColorToken): string;
  txt(text: string, x: number, y: number, font: string, color: string, align?: CanvasTextAlign, alpha?: number, baseline?: CanvasTextBaseline): void;
  rr(x: number, y: number, w: number, h: number, r: number): void;
  poly(points: [number, number][]): void;
  led(x: number, y: number, color: string, r?: number, glow?: number): void;
  card(x: number, y: number, w: number, h: number, o?: { r?: number; bg?: string; border?: string }): void;
  iso(gx: number, gy: number, gz: number, ox: number, oy: number, s: number): [number, number];
  isoTile(gx: number, gy: number, gz: number, w: number, d: number, h: number, ox: number, oy: number, scale: number, o?: Record<string, unknown>): void;
  boxGeo(x: number, y: number, w: number, h: number, depth: number): { t: number; dx: number; dy: number; topPt(u: number, v: number): [number, number] };
  box(x: number, y: number, w: number, h: number, depth: number, o?: Record<string, unknown>): ReturnType<Kit['boxGeo']>;
  slab(x: number, y: number, color: string, o?: Record<string, unknown>): ReturnType<Kit['boxGeo']>;
  pat(): { green: CanvasPattern; red: CanvasPattern; grey: CanvasPattern; white: CanvasPattern };
  pill(x: number, y: number, text: string, o?: { bg?: string; fg?: string; align?: 'left' | 'center' | 'right'; h?: number; pad?: number; font?: string; dot?: string; border?: string; shadow?: boolean; alpha?: number }): number;
  pillWidth(text: string, font?: string, pad?: number): number;
  stipple(text: string, x: number, y: number, progress: number, o?: { font?: string; size?: number; color?: string; align?: 'left' | 'center'; alpha?: number }): void;
  title(n: number, name: string, t: number): void;
  terminal(x: number, y: number, w: number, h: number, command: string, typed: number, t: number, alpha?: number): void;
  cylinder(cx: number, cy: number, r: number, h: number, rotation: number, o?: { glow?: number }): void;
  hourglass(cx: number, cy: number, r: number, h: number, rotation: number, fill: number): void;
  ribbon(cx: number, cy: number, length: number, alpha: number, t: number, dense?: boolean): void;
  record(cx: number, cy: number, alpha?: number, scale?: number): void;
  pipeHW(x: number): number;
  pipeBody(cy: number): void;
  pipePath(cy: number): void;
  pipeOutline(cy: number): void;
  ease: Easing;
  prog(t: number, a: number, b: number): number;
  clamp(v: number, a?: number, b?: number): number;
  lerp(a: number, b: number, t: number): number;
  hash(i: number): number;
}
export interface Easing { outCubic(t: number): number; inCubic(t: number): number; inOutCubic(t: number): number; outBack(t: number): number }

/** One segment of an episode: a scene drawn on the canvas, or a beat (a one-sentence pause in the expanded deck). */
export interface Segment {
  /** Scene name, shown uppercase in the deck and the scene title. Beats have none. */
  name?: string;
  /** Seconds. Scenes run 6.5–9, beats 2.5. */
  dur?: number;
  /** Narration. Wrap code in backticks to make a code pill. */
  text: string;
  beat?: boolean;
  /** Draws the scene at local time t (seconds), in the 470-wide canvas space. Beats show the previous scene's last frame. */
  draw?(K: Kit, t: number, dur: number, name: string, n: number): void;
}

export interface EpisodeProps { series?: string; episode?: number; segments: Segment[]; autoplay?: boolean; storageKey?: string; width?: number | string }
export declare function Episode(props: EpisodeProps): React.ReactElement;

export interface FrameProps { width?: number | string; className?: string; children?: React.ReactNode }
export declare function Frame(props: FrameProps): React.ReactElement;

export interface HeaderProps { series?: string; episode?: number; scene?: number; total?: number; docked?: boolean }
export declare function Header(props: HeaderProps): React.ReactElement;

export interface DeckProps { label?: string; clock?: string; text: string; current?: number; beat?: boolean; docked?: boolean; style?: React.CSSProperties; children?: React.ReactNode }
export declare function Deck(props: DeckProps): React.ReactElement;

export interface DeckButtonProps extends React.ButtonHTMLAttributes<HTMLButtonElement> { on?: boolean }
export declare function DeckButton(props: DeckButtonProps): React.ReactElement;

export interface ScrubberProps { max?: number; value?: number; ticks?: Array<number | { at: number; beat?: boolean }>; label?: string; id?: string; onChange?(value: number): void }
export declare function Scrubber(props: ScrubberProps): React.ReactElement;

export interface SceneTitleProps { n: number; name: string; width?: number; animate?: boolean }
export declare function SceneTitle(props: SceneTitleProps): React.ReactElement;

export interface StippleTextProps { text: string; family?: 'mono' | 'serif'; size?: number; weight?: number; italic?: boolean; color?: ColorToken; align?: 'left' | 'center'; width?: number; duration?: number; animate?: boolean }
export declare function StippleText(props: StippleTextProps): React.ReactElement;

export interface PillProps { tone?: 'ink' | 'blue' | 'green' | 'red' | 'amber' | 'outline' | 'k1' | 'k2' | 'k3' | 'k4'; size?: 'key' | 'sm' | 'lg'; dot?: boolean; children?: React.ReactNode }
export declare function Pill(props: PillProps): React.ReactElement;

export interface CardProps { eyebrow?: React.ReactNode; tone?: 'muted' | 'red' | 'green'; className?: string; style?: React.CSSProperties; children?: React.ReactNode }
export declare function Card(props: CardProps): React.ReactElement;

export interface StatProps { value: React.ReactNode; label?: React.ReactNode; note?: React.ReactNode; detail?: React.ReactNode; tone?: 'ink' | 'red' | 'green'; size?: 'xl' | 'lg' | 'md' | 'delta' }
export declare function Stat(props: StatProps): React.ReactElement;

export interface MetricRowsProps { rows: Array<{ label: React.ReactNode; value: React.ReactNode; tone?: 'ink' | 'red' | 'green' }> }
export declare function MetricRows(props: MetricRowsProps): React.ReactElement;

export interface ProgressBarProps { value: number; stalled?: boolean; caption?: React.ReactNode; label?: string }
export declare function ProgressBar(props: ProgressBarProps): React.ReactElement;

export interface StepListProps { steps: Array<{ label: React.ReactNode; cost?: React.ReactNode }>; current?: number; progress?: number }
export declare function StepList(props: StepListProps): React.ReactElement;

export interface TaskBarsProps { tasks: Array<{ label?: string; wait?: number; work?: number }>; barrier?: number; label?: string }
export declare function TaskBars(props: TaskBarsProps): React.ReactElement;

export interface DataGridProps { columns: string[]; rows: Array<Array<string | { v: string; tone?: 'green' | 'red' }>>; keepRows?: number[]; keepColumns?: number[]; filtered?: boolean; marked?: boolean; oblique?: boolean }
export declare function DataGrid(props: DataGridProps): React.ReactElement;

export interface TerminalProps { command: string; typed?: number; cursor?: boolean }
export declare function Terminal(props: TerminalProps): React.ReactElement;

export interface CodeCompareSide { title?: string; code: string[]; note?: string }
export interface CodeCompareProps { bad: CodeCompareSide; good: CodeCompareSide; struck?: boolean }
export declare function CodeCompare(props: CodeCompareProps): React.ReactElement;

export interface RecordProps { title?: string; field?: string; rest?: string }
export declare function Record(props: RecordProps): React.ReactElement;

export interface MachineProps { label?: string; leds?: ColorToken[]; color?: ColorToken; w?: number; h?: number; depth?: number; dividers?: number; vents?: number; pattern?: 'grey' | 'green' | 'red' | 'white'; wire?: boolean; topLabel?: string; labelColor?: ColorToken; animate?: boolean }
export declare function Machine(props: MachineProps): React.ReactElement;

export interface SlabProps { stack?: Array<'k1' | 'k2' | 'k3' | 'k4' | 'idle' | 'fact'>; scale?: number; labels?: boolean }
export declare function Slab(props: SlabProps): React.ReactElement;

export interface RackGridProps { size?: number; scale?: number; slow?: boolean; stragglers?: number; animate?: boolean }
export declare function RackGrid(props: RackGridProps): React.ReactElement;

export interface DiskProps { active?: boolean; label?: string | false; r?: number; h?: number; animate?: boolean }
export declare function Disk(props: DiskProps): React.ReactElement;

export interface HourglassProps { fill?: number; animate?: boolean }
export declare function Hourglass(props: HourglassProps): React.ReactElement;

export interface LEDProps { color?: ColorToken; size?: number; glow?: number; animate?: boolean }
export declare function LED(props: LEDProps): React.ReactElement;

export interface BitRibbonProps { length?: number; dense?: boolean; animate?: boolean }
export declare function BitRibbon(props: BitRibbonProps): React.ReactElement;

export interface PipeProps { mode?: 'jammed' | 'flowing'; badge?: string; readout?: boolean; animate?: boolean }
export declare function Pipe(props: PipeProps): React.ReactElement;

export interface ClosingCardProps { series?: string; episode?: number; title?: string; subtitle?: string; takeaways?: Array<{ label: string; tone?: 'green' | 'blue' | 'amber' | 'red' | 'ink' }>; footnote?: string }
export declare function ClosingCard(props: ClosingCardProps): React.ReactElement;

declare global {
  interface Window {
    SparkInternals: {
      Episode: typeof Episode; Frame: typeof Frame; Header: typeof Header; Deck: typeof Deck; DeckButton: typeof DeckButton; Scrubber: typeof Scrubber;
      SceneTitle: typeof SceneTitle; StippleText: typeof StippleText; Pill: typeof Pill; Card: typeof Card; Stat: typeof Stat; MetricRows: typeof MetricRows;
      ProgressBar: typeof ProgressBar; StepList: typeof StepList; TaskBars: typeof TaskBars; DataGrid: typeof DataGrid; Terminal: typeof Terminal;
      CodeCompare: typeof CodeCompare; Record: typeof Record; Machine: typeof Machine; Slab: typeof Slab; RackGrid: typeof RackGrid; Disk: typeof Disk;
      Hourglass: typeof Hourglass; LED: typeof LED; BitRibbon: typeof BitRibbon; Pipe: typeof Pipe; ClosingCard: typeof ClosingCard;
      /** Bind the drawing kit to a canvas context. Colors come from tokens.css unless you pass them. */
      kit(ctx: CanvasRenderingContext2D, colors?: Record<string, string>): Kit;
      /** Read the token colors the kit uses from tokens.css (with the episode's values as fallback). */
      tokens(el?: Element): Record<string, string>;
      ease: Easing;
      clamp(v: number, a?: number, b?: number): number;
      lerp(a: number, b: number, t: number): number;
      prog(t: number, a: number, b: number): number;
      hash(i: number): number;
      tokenize(text: string): Array<{ w?: string; code?: string; tail?: string }>;
    };
  }
}
