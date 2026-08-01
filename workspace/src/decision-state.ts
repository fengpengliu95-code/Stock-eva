import {
  fetchMarketRegime,
  fetchSectorLeaders,
  fetchSectorRotation,
  type MarketRegimeResponse,
  type SectorLeadersResponse,
  type SectorRotationResponse,
} from "./decision-api";

export interface DecisionQuery {
  asOf: string;
  taxonomyId: string;
}

export interface LeaderQuery extends DecisionQuery {
  sectorId: string;
}

export interface DecisionOverview {
  market: MarketRegimeResponse;
  sectors: SectorRotationResponse;
}

export class DecisionRequestManager {
  private overviewController: AbortController | null = null;
  private leaderController: AbortController | null = null;

  constructor(private readonly fetcher: typeof fetch = fetch) {}

  async loadOverview(query: DecisionQuery): Promise<DecisionOverview | null> {
    this.overviewController?.abort();
    this.leaderController?.abort();
    const controller = new AbortController();
    this.overviewController = controller;
    try {
      const [market, sectors] = await Promise.all([
        fetchMarketRegime(query.asOf, this.fetcher, controller.signal),
        fetchSectorRotation(
          query.asOf,
          query.taxonomyId,
          this.fetcher,
          controller.signal,
        ),
      ]);
      if (
        controller.signal.aborted ||
        this.overviewController !== controller
      ) {
        return null;
      }
      return { market, sectors };
    } catch (error) {
      if (
        controller.signal.aborted ||
        this.overviewController !== controller
      ) {
        return null;
      }
      throw error;
    } finally {
      if (this.overviewController === controller) {
        this.overviewController = null;
      }
    }
  }

  async loadLeaders(
    query: LeaderQuery,
  ): Promise<SectorLeadersResponse | null> {
    this.leaderController?.abort();
    const controller = new AbortController();
    this.leaderController = controller;
    try {
      const result = await fetchSectorLeaders(
        query.asOf,
        query.taxonomyId,
        query.sectorId,
        this.fetcher,
        controller.signal,
      );
      if (controller.signal.aborted || this.leaderController !== controller) {
        return null;
      }
      return result;
    } catch (error) {
      if (controller.signal.aborted || this.leaderController !== controller) {
        return null;
      }
      throw error;
    } finally {
      if (this.leaderController === controller) {
        this.leaderController = null;
      }
    }
  }

  dispose(): void {
    this.overviewController?.abort();
    this.leaderController?.abort();
    this.overviewController = null;
    this.leaderController = null;
  }
}
