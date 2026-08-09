import type { DecisionContext, DecisionReturnView, SourceView } from "./state";

const SYMBOL_PATTERN = /^(sh|sz)\.[0-9]{6}$/;
const ISO_DATE_PATTERN = /^\d{4}-\d{2}-\d{2}$/;
const CANONICAL_TAXONOMY = "baostock.industry_classification";

export interface SecurityRoute {
  symbol: string;
  sourceView: SourceView;
  asOf?: string;
  taxonomyId?: string;
  sectorId?: string;
  returnView?: DecisionReturnView;
}

export interface DecisionRoute {
  asOf: string;
  taxonomyId: string;
  sectorId: string | null;
}

export interface SectorRoute extends DecisionRoute {}

function isSourceView(value: string | null): value is SourceView {
  return (
    value === "portfolio" || value === "watchlists" || value === "sectors"
  );
}

function isDecisionReturnView(value: string | null): value is DecisionReturnView {
  return value === "overview" || value === "sectors";
}

function isValidAsOf(value: string): boolean {
  if (!ISO_DATE_PATTERN.test(value)) return false;
  const [year, month, day] = value.split("-").map(Number);
  const date = new Date(Date.UTC(year, month - 1, day));
  return (
    date.getUTCFullYear() === year &&
    date.getUTCMonth() === month - 1 &&
    date.getUTCDate() === day
  );
}

function isCanonicalTaxonomy(value: string): boolean {
  return value === CANONICAL_TAXONOMY;
}

function parseQuery(raw: string): URLSearchParams | null {
  try {
    // URLSearchParams replaces malformed percent sequences instead of rejecting
    // them. Validate raw UTF-8 first so bad route input never reaches APIs.
    decodeURIComponent(raw.replace(/\+/g, " "));
    return new URLSearchParams(raw);
  } catch (error) {
    if (error instanceof URIError) return null;
    throw error;
  }
}

function validDecisionContext(
  value: Partial<DecisionContext>,
): value is DecisionContext {
  return (
    typeof value.asOf === "string" &&
    isValidAsOf(value.asOf) &&
    typeof value.taxonomyId === "string" &&
    isCanonicalTaxonomy(value.taxonomyId) &&
    typeof value.sectorId === "string" &&
    value.sectorId.length > 0 &&
    (value.returnView === undefined ||
      value.returnView === "overview" ||
      value.returnView === "sectors")
  );
}

function routeHash(prefix: "overview" | "sectors", route: DecisionRoute): string {
  if (
    !isValidAsOf(route.asOf) ||
    !isCanonicalTaxonomy(route.taxonomyId) ||
    route.sectorId === ""
  ) {
    throw new TypeError("invalid decision route");
  }
  const query = new URLSearchParams({
    as_of: route.asOf,
    taxonomy_id: route.taxonomyId,
  });
  if (route.sectorId !== null) query.set("sector_id", route.sectorId);
  return `#${prefix}?${query}`;
}

function parseRouteHash(
  prefix: "overview" | "sectors",
  hash: string,
): DecisionRoute | null {
  const match = new RegExp(`^#${prefix}(?:\\?(.*))?$`).exec(hash);
  if (!match) return null;
  const query = parseQuery(match[1] ?? "");
  if (!query) return null;
  const asOf = query.get("as_of");
  const taxonomyId = query.get("taxonomy_id");
  const sectorId = query.get("sector_id");
  if (
    asOf === null ||
    !isValidAsOf(asOf) ||
    taxonomyId === null ||
    !isCanonicalTaxonomy(taxonomyId) ||
    sectorId === ""
  ) {
    return null;
  }
  return { asOf, taxonomyId, sectorId };
}

export function decisionHash(route: DecisionRoute): string {
  return routeHash("overview", route);
}

export function parseDecisionHash(hash: string): DecisionRoute | null {
  return parseRouteHash("overview", hash);
}

export function sectorHash(route: SectorRoute): string {
  return routeHash("sectors", route);
}

export function parseSectorHash(hash: string): SectorRoute | null {
  return parseRouteHash("sectors", hash);
}

export function securityHash(
  symbol: string,
  sourceView: SourceView,
  decisionContext?: DecisionContext,
): string {
  if (!SYMBOL_PATTERN.test(symbol)) throw new TypeError("invalid security symbol");
  if (decisionContext !== undefined && !validDecisionContext(decisionContext)) {
    throw new TypeError("invalid decision context");
  }
  const query = new URLSearchParams({ from: sourceView });
  if (decisionContext) {
    query.set("as_of", decisionContext.asOf);
    query.set("taxonomy_id", decisionContext.taxonomyId);
    query.set("sector_id", decisionContext.sectorId);
    if (decisionContext.returnView) {
      query.set("return_view", decisionContext.returnView);
    }
  }
  return `#security/${encodeURIComponent(symbol)}?${query}`;
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
  const query = parseQuery(match[2] ?? "");
  if (!query) return null;
  const sourceView = query.get("from");
  if (!SYMBOL_PATTERN.test(symbol) || !isSourceView(sourceView)) return null;
  const asOf = query.get("as_of");
  const taxonomyId = query.get("taxonomy_id");
  const sectorId = query.get("sector_id");
  const returnView = query.get("return_view");
  const hasDecisionContext =
    asOf !== null || taxonomyId !== null || sectorId !== null;
  if (!hasDecisionContext) {
    return returnView === null ? { symbol, sourceView } : null;
  }
  if (
    !validDecisionContext({
      asOf: asOf ?? undefined,
      taxonomyId: taxonomyId ?? undefined,
      sectorId: sectorId ?? undefined,
    })
  ) {
    return null;
  }
  if (returnView !== null && !isDecisionReturnView(returnView)) return null;
  return {
    symbol,
    sourceView,
    asOf: asOf!,
    taxonomyId: taxonomyId!,
    sectorId: sectorId!,
    ...(returnView ? { returnView } : {}),
  };
}

export function bindSecurityEntrypoints(
  root: Document,
  navigate: (
    symbol: string,
    sourceView: SourceView,
    decisionContext?: DecisionContext,
  ) => void,
): () => void {
  const routeFor = (
    target: EventTarget | null,
  ):
    | {
        symbol: string;
        sourceView: SourceView;
        decisionContext?: DecisionContext;
      }
    | null => {
    if (!(target instanceof Element)) return null;
    const control = target.closest<HTMLElement>("[data-security-symbol]");
    if (!control) return null;
    const symbol = control.dataset.securitySymbol?.toLowerCase() ?? "";
    const source = control.dataset.securitySource ?? "";
    if (!SYMBOL_PATTERN.test(symbol) || !isSourceView(source)) return null;
    const context = {
      asOf: control.dataset.securityAsOf,
      taxonomyId: control.dataset.securityTaxonomy,
      sectorId: control.dataset.securitySector,
    };
    if (Object.values(context).some((value) => value !== undefined)) {
      if (!validDecisionContext(context)) return null;
      return { symbol, sourceView: source, decisionContext: context };
    }
    return { symbol, sourceView: source };
  };
  const performNavigation = (
    route: NonNullable<ReturnType<typeof routeFor>>,
  ): void => {
    if (route.decisionContext) {
      navigate(route.symbol, route.sourceView, route.decisionContext);
    } else {
      navigate(route.symbol, route.sourceView);
    }
  };
  const onClick = (event: MouseEvent): void => {
    const route = routeFor(event.target);
    if (!route) return;
    event.preventDefault();
    performNavigation(route);
  };
  const onKeyDown = (event: KeyboardEvent): void => {
    if (event.repeat || (event.key !== "Enter" && event.key !== " ")) return;
    const route = routeFor(event.target);
    if (!route) return;
    event.preventDefault();
    performNavigation(route);
  };
  root.addEventListener("click", onClick);
  root.addEventListener("keydown", onKeyDown);
  return () => {
    root.removeEventListener("click", onClick);
    root.removeEventListener("keydown", onKeyDown);
  };
}
