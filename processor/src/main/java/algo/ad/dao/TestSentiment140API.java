package algo.ad.dao;

import java.util.ArrayList;

import org.springframework.web.client.RestTemplate;

public class TestSentiment140API {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		RestTemplate restTemplate = new RestTemplate();
//		Sentiment140RequestDAO page = restTemplate.getForObject(
//				"http://graph.facebook.com/pivotalsoftware",
//				Sentiment140RequestDAO.class);

		Sentiment140RequestDAO requestObject = new Sentiment140RequestDAO();
		ArrayList<Sentiment140Request> requestData = new ArrayList<Sentiment140Request>();

		Sentiment140Request request = new Sentiment140Request();
		request.setId("1");
		request.setQuery("#BieberRoast");
		request.setText("RT @hannibalburess: This one got cut from #BieberRoast. I get it. http://t.co/mTKExQEuuM");
		requestData.add(request);

		request = new Sentiment140Request();
		request.setId("2");
		request.setQuery("#BieberRoast");
		request.setText("RT @justinbieber: The #BieberRoast starts now on @ComedyCentral !! :) I'm screwed. Lol");
		requestData.add(request);

		request = new Sentiment140Request();
		request.setId("3");
		request.setQuery("#MakingAustraliaGreat");
		request.setText("RT @whatamindblast: Labor saved the economy in 2008/9. That is a fact. #MakingAustraliaGreat");
		requestData.add(request);

		request = new Sentiment140Request();
		request.setId("4");
		request.setQuery("#MakingAustraliaGreat");
		request.setText("@ABCTV Enjoyed every minute of #MakingAustraliaGreat. Such an amazing insight into our past & future.  Something every OZ should watch.");
		requestData.add(request);
		
		requestObject.setData(requestData);
		//restTemplate.getMessageConverters().add(new MappingJackson2HttpMessageConverter());
		Sentiment140ResponseDAO response = restTemplate
				.postForObject(
						"http://www.sentiment140.com/api/bulkClassifyJson?appid=usra2013twitter@gmail.com",
						requestObject, Sentiment140ResponseDAO.class);
		System.out.println("Name:    " + response.getData());
		// System.out.println("About:   " + page.getAbout());
		// System.out.println("Phone:   " + page.getPhone());
		// System.out.println("Website: " + page.getWebsite());
	}

}
