import {
  parse,
  validate,
  execute,
  GraphQLSchema,
  DocumentNode,
  GraphQLError,
} from "graphql"
import { debounce } from "lodash"
import { Store } from "redux"
import withResolverContext from "../schema/context"
import { LocalNodeModel } from "../schema/node-model"
import nodeStore from "../db/nodes"
import createPageDependency from "../db/nodes"

interface IGraphQLRunnerStats {
  totalQueries: number
  uniqueOperations: Set<string>
  uniqueQueries: Set<string>
  totalRunQuery: number
  totalPluralRunQuery: number
  totalIndexHits: number
  totalNonSingleFilters: number
  comparatorsUsed: Map<string, string>
  uniqueFilterPaths: Set<string>
  uniqueSorts: Set<string>
}

export default class GraphQLRunner {
  store: Store
  nodeModel: LocalNodeModel
  schema: GraphQLSchema
  parseCache: Map<any, any>
  validDocuments: WeakSet<any>
  stats: IGraphQLRunnerStats
  scheduleClearCache: any

  constructor(store: Store) {
    this.store = store
    const { schema, schemaCustomization } = this.store.getState()

    this.nodeModel = new LocalNodeModel({
      nodeStore,
      schema,
      schemaComposer: schemaCustomization.composer,
      createPageDependency,
    })
    this.schema = schema
    this.parseCache = new Map()
    this.validDocuments = new WeakSet()
    this.scheduleClearCache = debounce(this.clearCache.bind(this), 5000)

    this.stats = {
      totalQueries: 0,
      uniqueOperations: new Set(),
      uniqueQueries: new Set(),
      totalRunQuery: 0,
      totalPluralRunQuery: 0,
      totalIndexHits: 0,
      totalNonSingleFilters: 0,
      comparatorsUsed: new Map(),
      uniqueFilterPaths: new Set(),
      uniqueSorts: new Set(),
    }
  }

  clearCache(): void {
    this.parseCache.clear()
    this.validDocuments = new WeakSet()
  }

  parse(query): DocumentNode {
    if (!this.parseCache.has(query)) {
      this.parseCache.set(query, parse(query))
    }
    return this.parseCache.get(query)
  }

  validate(schema: GraphQLSchema, document: DocumentNode): Array<GraphQLError> {
    if (!this.validDocuments.has(document)) {
      const errors = validate(schema, document)
      if (!errors.length) {
        this.validDocuments.add(document)
      }
      return errors as Array<GraphQLError>
    }
    return []
  }

  query(query: string, context: { [key: string]: any }): Promise<any> {
    const { schema, schemaCustomization } = this.store.getState()

    if (this.schema !== schema) {
      this.schema = schema
      this.clearCache()
    }

    this.stats.totalQueries++
    this.stats.uniqueOperations.add(`${query}${JSON.stringify(context)}`)
    this.stats.uniqueQueries.add(query)

    const document = this.parse(query)
    const errors = this.validate(schema, document)

    const result =
      errors.length > 0
        ? { errors }
        : execute({
            schema,
            document,
            rootValue: context,
            contextValue: withResolverContext({
              schema,
              schemaComposer: schemaCustomization.composer,
              context,
              customContext: schemaCustomization.context,
              nodeModel: this.nodeModel,
              stats: this.stats,
            }),
            variableValues: context,
          })

    // Queries are usually executed in batch. But after the batch is finished
    // cache just wastes memory without much benefits.
    // TODO: consider a better strategy for cache purging/invalidation
    this.scheduleClearCache()
    return Promise.resolve(result)
  }
}
