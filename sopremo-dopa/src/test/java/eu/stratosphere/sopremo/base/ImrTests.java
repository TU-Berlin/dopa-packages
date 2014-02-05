package eu.stratosphere.sopremo.base;

import net.internetmemory.commons.searching.AdaptedResponse;
import net.internetmemory.commons.searching.Query;
import net.internetmemory.commons.searching.QueryStringQuery;
import net.internetmemory.commons.searching.ResultResponse;
import net.internetmemory.service.APICredentials;
import net.internetmemory.service.lookup.LookupServiceClient;
import org.junit.Assert;
import org.junit.Test;

import java.util.Date;
import java.util.Map;

/**
 * Created by mleich on 27/01/14.
 */
public class ImrTests {

    // REST API root URL
    public static final String REST_API_ROOT_URI = "http://service.mignify.com/";

    // Sample API key
    public static final APICredentials SAMPLE_CREDENTIAL = new APICredentials("4p9sh6qc", "rolh2a4cgn6j1i1");

    @Test
    public void testServerResponse () {

        try {
            // Create new instance of full test search service client
            LookupServiceClient client = new LookupServiceClient();

            // Set the root URI of the full text search service
            client.setRestApiRootUri(REST_API_ROOT_URI);

            // Set the credential to use the service
            client.useCredentials(SAMPLE_CREDENTIAL);

            // Sample query: search pages whose text contents contain term "France".
            Query query = new QueryStringQuery("textContent:AOL");

            // Search contents in mignify:worldnews collection,
            // then fetch result with rowsTofetch:10, pageToFetch:1 and field:null (fetch all fields)
            AdaptedResponse res = client.searchContent("mignify", "dopaCleaning", query);

            // Print basic information
            System.out.println("Num of hits: " + res.hitNum);
            System.out.println("Num of fetched resources: " + res.totalFetchNum);
            System.out.println("Time (ms): " + res.totalTookMilis);

            int count = 0;

            // For each retrieved page....
            for(ResultResponse r: res.results){
                count++;

                // Get URL and timestamp of the page
                String url = r.getProperties().get("url");
                long timestamp = Long.valueOf(r.getProperties().get("timestamp"));

                // Print
                System.out.println("Result " + count + ": " + url + " (" + new Date(timestamp) + ")");

                // For each data field of the retrieved page...
                for(Map.Entry<String, String> e: r.getProperties().entrySet()){
                    //Print
                    System.out.println("\t" + e.getKey() + ": " + e.getValue());
                }
            }
        } catch (Exception e) {
            e.printStackTrace(System.err);
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testDomainReversal () {
        Assert.assertEquals("domain reverasl didn't work as expcected",
                "http://com.test.www/somepath.jpeg", ImrSearch.reverseDomain("http://www.test.com/somepath.jpeg"));
        Assert.assertEquals("domain reverasl didn't work as expcected",
                "http://com.test.www:8080/somepath.jpeg", ImrSearch.reverseDomain("http://www.test.com:8080/somepath.jpeg"));
        Assert.assertEquals("domain reverasl didn't work as expcected",
                "http://com.test.www.sub.subsub/somepath.jpeg", ImrSearch.reverseDomain("http://subsub.sub.www.test.com/somepath.jpeg"));
    }

}
