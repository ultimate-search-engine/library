package libraries

import co.elastic.clients.elasticsearch.ElasticsearchClient
import co.elastic.clients.elasticsearch._types.SortOrder
import co.elastic.clients.elasticsearch._types.mapping.ObjectProperty
import co.elastic.clients.elasticsearch._types.mapping.Property
import co.elastic.clients.elasticsearch._types.mapping.TypeMapping
import co.elastic.clients.elasticsearch._types.query_dsl.Query
import co.elastic.clients.elasticsearch.core.*
import co.elastic.clients.elasticsearch.core.bulk.IndexOperation
import co.elastic.clients.elasticsearch.core.search.Hit
import co.elastic.clients.elasticsearch.indices.*
import co.elastic.clients.json.jackson.JacksonJsonpMapper
import co.elastic.clients.transport.ElasticsearchTransport
import co.elastic.clients.transport.rest_client.RestClientTransport
import co.elastic.clients.util.ObjectBuilder
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.withContext
import org.apache.http.HttpHost
import org.apache.http.auth.AuthScope
import org.apache.http.auth.UsernamePasswordCredentials
import org.apache.http.client.CredentialsProvider
import org.apache.http.impl.client.BasicCredentialsProvider
import org.elasticsearch.client.RestClient


data class Credentials(val userName: String, val password: String)
data class Address(val hostname: String, val port: Int)

open class Elastic(credentials: Credentials, address: Address, val index: String) {
    private val client: ElasticsearchClient

    init {
        val credentialsProvider: CredentialsProvider = BasicCredentialsProvider()
        credentialsProvider.setCredentials(
            AuthScope.ANY, UsernamePasswordCredentials(credentials.userName, credentials.password)
        )

        val restClient = RestClient.builder(
            HttpHost(address.hostname, address.port)
        ).setHttpClientConfigCallback { httpClientBuilder ->
            httpClientBuilder.disableAuthCaching()
            httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
        }.build()

        val transport: ElasticsearchTransport = RestClientTransport(
            restClient, JacksonJsonpMapper()
        )
        client = ElasticsearchClient(transport)
    }


    val alias = Alias(client, index)

    suspend fun indexPage(page: Page.PageType, id: String? = null): IndexResponse = coroutineScope {
        withContext(
            Dispatchers.Default
        ) { client.index(indexRequest(page, id)) }
    }


    fun indexRequest(page: Page.PageType, id: String? = null) = IndexRequest.of<Page.PageType> {
        if (id != null) it.id(id)
        it.index(index)
        it.document(page)
    }


    suspend fun docsByUrlOrNull(url: String, batchSize: Long = 10): List<Hit<Page.PageType>>? = coroutineScope {
        val search: SearchResponse<Page.PageType> = withContext(Dispatchers.Default) {
            search(docsByUrlRequest(url, batchSize))
        }
        val hits = search.hits().hits()
        return@coroutineScope hits.ifEmpty { null }
    }

    fun docsByUrlOrNullBulk(urls: List<String>, batchSize: Long = 10): MsearchResponse<Page.PageType>? {
        fun searchesBuilder(req: MsearchRequest.Builder, url: String): ObjectBuilder<MsearchRequest> =
            req.searches { searches ->
                searches.header {
                    it.index(index)
                }
                searches.body {
                    it.size(batchSize.toInt())
                    it.query { query ->
                        query.ids { ids ->
                            ids.values(urls)
                        }
                        query.term { term ->
                            term.field("address.url").value { value ->
                                value.stringValue(url)
                            }
                        }
                    }
                }
            }

        val req = MsearchRequest.of { builder ->
            urls.forEach { searchesBuilder(builder, it) }
            builder.index(index)
        }
        return client.msearch(req, Page.PageType::class.java)
    }


    fun docsByUrlRequest(url: String, batchSize: Long = 10): SearchRequest = SearchRequest.of {
        it.size(batchSize.toInt())
        it.index(index).query { query ->
            query.term { term ->
                term.field("address.url").value { value ->
                    value.stringValue(url)
                }
            }
        }
    }

    data class PageById(val page: Page.PageType, val id: String?)

    fun indexDocsBulkByIds(docs: List<PageById>): BulkResponse {
        fun indexBuilder(req: BulkRequest.Builder, doc: PageById) {
            req.operations { operations ->
                operations.index(IndexOperation.of<Page.PageType> { index ->
                    if (doc.id != null) index.id(doc.id)
                    index.document(doc.page)
                    index.index(this.index)
                })
            }
        }

        val req = BulkRequest.of { builder ->
            docs.forEach { indexBuilder(builder, it) }
            builder.index(index)
        }
        return client.bulk(req)
    }

    fun indexDocsBulk(pages: List<Page.PageType>) = indexDocsBulkByIds(pages.map { PageById(it, null) })


    suspend fun getAllDocsCount(): Long = coroutineScope {
        val search: CountRequest = CountRequest.of {
            it.index(index)
            it.query { query ->
                query.bool { bool ->
                    bool.must { must ->
                        must.exists { exists ->
                            exists.field("address.url")
                        }
                    }
                }
            }
        }
        return@coroutineScope client.count(search).count()
    }


    suspend fun search(searchRequest: SearchRequest): SearchResponse<Page.PageType> = coroutineScope {
        return@coroutineScope withContext(Dispatchers.IO) { client.search(searchRequest, Page.PageType::class.java) }
    }


    suspend fun putMapping(numberOfShards: Int = 1, numberOfReplicas: Int = 0): CreateIndexResponse = coroutineScope {

        fun complexMappingType(name: String, parent: ObjectProperty.Builder): ObjectProperty.Builder? {
            return parent.properties(name) { property2 ->
                property2.text { text ->
                    text.analyzer("standard")
                    text.fields("searchAsYouType", Property.of { field ->
                        field.searchAsYouType { text ->
                            text.analyzer("standard")
                        }
                    })
                }
            }
        }

        fun keywordProperty(name: String, parent: ObjectProperty.Builder): ObjectProperty.Builder? {
            return parent.properties(name, Property.of { property ->
                property.keyword { it }
            })
        }

        fun textProperty(name: String, parent: ObjectProperty.Builder): ObjectProperty.Builder? {
            return parent.properties(name, Property.of { property ->
                property.text { text ->
                    text.analyzer("standard")
                }
            })
        }

        fun doubleProperty(name: String, parent: ObjectProperty.Builder): ObjectProperty.Builder? {
            return parent.properties(name, Property.of { property ->
                property.double_ { double ->
                    double.nullValue(0.0)
                }
            })
        }

        val mappingRequest = CreateIndexRequest.of { indexRequest ->
            indexRequest.index(index)
            indexRequest.settings { settings ->
                settings.numberOfShards(numberOfShards.toString())
                settings.numberOfReplicas(numberOfReplicas.toString())
//                settings.analysis { analysis ->
//                    analysis.analyzer("standard", Analyzer.of { analyzer ->
//                        analyzer.language { language ->
//                            language.language(Language.English)
//                            language.stemExclusion(listOf(".", " ", ","))
//                        }
//                    })
//                }
            }
            indexRequest.mappings(TypeMapping.of { typeMapping ->
//                typeMapping.indexField(IndexField.of {
//                    it.enabled(true)
//                })
                typeMapping.properties("metadata", Property.of { property ->
                    property.`object` {
                        complexMappingType("title", it)
                        complexMappingType("description", it)

                        textProperty("openGraphTitle", it)
                        textProperty("openGraphDescription", it)

                        keywordProperty("openGraphImgURL", it)
                        keywordProperty("type", it)
                        keywordProperty("tags", it)
                    }
                })
                typeMapping.properties("body", Property.of { property ->
                    property.`object` { obj ->
                        obj.properties("headings", Property.of { property ->
                            property.`object` { obj ->
                                complexMappingType("h1", obj)

                                textProperty("h2", obj)
                                textProperty("h3", obj)
                                textProperty("h4", obj)
                                textProperty("h5", obj)
                                textProperty("h6", obj)
                            }
                        })
                        textProperty("article", obj)
                        textProperty("boldText", obj)
                        obj.properties("links", Property.of { property ->
                            property.`object` { obj ->
                                obj.properties("internal", Property.of { property ->
                                    property.`object` { obj ->
                                        textProperty("text", obj)
                                        textProperty("href", obj)
                                    }
                                })
                                obj.properties("external", Property.of { property ->
                                    property.`object` { obj ->
                                        textProperty("text", obj)
                                        textProperty("href", obj)
                                    }
                                })
                            }
                        })
                    }
                })
                typeMapping.properties("inferredData", Property.of { property ->
                    property.`object` { obj ->
                        obj.properties("backLinks", Property.of { property ->
                            property.`object` { obj ->
                                textProperty("text", obj)
                                keywordProperty("source", obj)
                            }
                        })
                        obj.properties("ranks", Property.of { property ->
                            property.`object` { obj ->
                                doubleProperty("pagerank", obj)
                                obj.properties("smartRank", Property.of { property ->
                                    property.rankFeature {
                                        it.positiveScoreImpact(true)
                                    }
                                })
                            }
                        })
                        obj.properties("domainName", Property.of { property ->
                            property.text {
                                it.analyzer("standard")
                            }
                        })
                    }
                })
                typeMapping.properties("address", Property.of {
                    it.`object` { obj ->
                        keywordProperty("url", obj)
                        textProperty("urlAsText", obj)
                    }
                })
                typeMapping.properties("crawlerStatus", Property.of {
                    it.keyword { keyword ->
                        keyword.ignoreAbove(256)
                    }
                })
                typeMapping.properties("crawlerTimestamp", Property.of {
                    it.date { date ->
                        date.nullValue("0")
                    }
                })
            })
        }
        client.indices().create(mappingRequest)
    }

    fun deleteIndex(index: String = this.index) {
        client.indices().delete(DeleteIndexRequest.of {
            it.index(index)
        })
    }

    private fun searchAfterUrlRequest(batchSize: Long, afterUrl: String?): SearchRequest =
        SearchRequest.of { searchReq ->
            searchReq.index(index)
            searchReq.query { query ->
                query.bool { bool ->
                    bool.must { must ->
                        must.exists {
                            it.field("address.url")
                        }
                    }
                }
            }
            if (afterUrl != null) searchReq.searchAfter(afterUrl)
            searchReq.size(batchSize.toInt())
            searchReq.sort { sort ->
                sort.field {
                    it.field("address.url")
                    it.order(SortOrder.Asc)
                }
            }
        }

    suspend fun searchAfterUrl(batchSize: Long, url: String?) =
        search(searchAfterUrlRequest(batchSize, url))

}


class Alias(private val client: ElasticsearchClient, private val index: String) {

    fun create(alias: String) {
        client.indices().putAlias(PutAliasRequest.of {
            it.index(index)
            it.name(alias)
        })
    }

    fun delete(alias: String) {
        getIndexByAlias(alias).forEach { index ->
            client.indices().deleteAlias(DeleteAliasRequest.of {
                it.index(index)
                it.name(alias)
            })
        }
    }

    fun getIndexByAlias(alias: String): MutableSet<String> {
        return try {
            client.indices().getAlias(GetAliasRequest.of {
                it.name(alias)
            }).result().keys
        } catch (e: Exception) {
            mutableSetOf(alias)
        }
    }
}


//suspend fun main() {
//    val es = Elastic(Credentials("elastic", "testerino"), Address("localhost", 9200), "search")
//    val builder = SearchRequest.of { s ->
//        s.index(es.index)
//        s.query { query ->
//            query.bool { bool ->
//                bool.must(listOf(Query.of { query ->
//                    query.multiMatch { mm ->
//                        mm.query("youtube")
//                        mm.fields(
//                            "metadata.title^2",
//                            "metadata.description",
//                            "inferredData.backLinks.text^2",
//                            "body.headings.h1",
//                        )
//                        mm.fuzziness("3")
//                    }
//                }, Query.of { query ->
//                    query.rankFeature {
//                        it.field("inferredData.ranks.smartRank")
//                        it.linear { it }
//                    }
//                }))
//            }
//        }
//    }
//    val a = es.search(builder)
//    a.hits()?.hits()?.forEach { println(it.source()?.address?.url) }
//    println("done")
//
//}
