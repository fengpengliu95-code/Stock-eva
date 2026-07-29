import type { SourceView } from "./state";

const SYMBOL_PATTERN = /^(sh|sz)\.[0-9]{6}$/;

export interface SecurityRoute {
  symbol: string;
  sourceView: SourceView;
}

function isSourceView(value: string | null): value is SourceView {
  return value === "portfolio" || value === "watchlists";
}

export function securityHash(symbol: string, sourceView: SourceView): string {
  if (!SYMBOL_PATTERN.test(symbol)) throw new TypeError("invalid security symbol");
  return `#security/${encodeURIComponent(symbol)}?from=${sourceView}`;
}

export function parseSecurityHash(hash: string): SecurityRoute | null {
  const match = /^#security\/([^?]+)(?:\?(.*))?$/.exec(hash);
  if (!match) return null;
  let symbol: string;
  try {
    symbol = decodeURIComponent(match[1]).toLowerCase();
  } catch (error) {
    if (error instanceof URIError) return null;
    throw error;
  }
  const sourceView = new URLSearchParams(match[2] ?? "").get("from");
  if (!SYMBOL_PATTERN.test(symbol) || !isSourceView(sourceView)) return null;
  return { symbol, sourceView };
}

export function bindSecurityEntrypoints(
  root: Document,
  navigate: (symbol: string, sourceView: SourceView) => void,
): () => void {
  const routeFor = (
    target: EventTarget | null,
  ): { symbol: string; sourceView: SourceView } | null => {
    if (!(target instanceof Element)) return null;
    const control = target.closest<HTMLElement>("[data-security-symbol]");
    if (!control) return null;
    const symbol = control.dataset.securitySymbol?.toLowerCase() ?? "";
    const source = control.dataset.securitySource ?? "";
    if (!SYMBOL_PATTERN.test(symbol) || !isSourceView(source)) return null;
    return { symbol, sourceView: source };
  };
  const onClick = (event: MouseEvent): void => {
    const route = routeFor(event.target);
    if (!route) return;
    event.preventDefault();
    navigate(route.symbol, route.sourceView);
  };
  const onKeyDown = (event: KeyboardEvent): void => {
    if (event.repeat || (event.key !== "Enter" && event.key !== " ")) return;
    const route = routeFor(event.target);
    if (!route) return;
    event.preventDefault();
    navigate(route.symbol, route.sourceView);
  };
  root.addEventListener("click", onClick);
  root.addEventListener("keydown", onKeyDown);
  return () => {
    root.removeEventListener("click", onClick);
    root.removeEventListener("keydown", onKeyDown);
  };
}
