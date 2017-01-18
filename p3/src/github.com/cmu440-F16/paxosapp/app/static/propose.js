var retriesCount = 0
var retriesLimit = 5

$('.proposalForm').submit(function() {
    data = {}
    data["id"] = $(".id").val()
    data["name"] = $(".selected").text()
    $("input#button").hide()
    $("#youtube-search").hide()
    $.ajax({
        type : "POST",
        url: "post_proposal",
        data: JSON.stringify(data),
        contentType: 'application/json; charset=utf-8',
        success: function(data) {
            if (data["success"] == true) {
                $("ul.log").append("<li>Got position number "+data["index"]+"!</li>")
            } else {
                if (retriesCount == retriesLimit) {
                    $("ul.log").append("<li>Looks like too many of our servers have died, sorry. Try again later.</li>")
                    return
                }
                retriesCount++
                $("ul.log").append("<li>Failed to get position number "+data["index"]+"... Retrying</li>")
                $.ajax(this)
            }
        }
    })
    return false
})


$("#youtube-resultsList").on('click', 'li', function() {
    $(this).addClass("selected").siblings().removeClass("selected");
    $('#proposalID').val($(this).attr('data-id'));
    console.log($(this).text())
});
// when they click on the search button...
$("#youtube-searchButton").click(function() {
    fetchVideos();
});

//allow them start search by hitting enter key:
$("#youtube-searchBox").focus(function() {
    $(this).keyup(function(e) {
        if(e.which == 13) {
            fetchVideos();
            console.log('enter key pressed');
        }
    });
});

//main function. it makes youtube APIcall and populates results container
function fetchVideos(){
    //empty the results container
    $('#youtube-resultsList').empty();

    // construct the youtubeAPI search query:
    var search_input = $('#youtube-searchBox').val();
    var keyword = encodeURIComponent(search_input);
    var yt_url='https://www.googleapis.com/youtube/v3/search?part=snippet&type=video&q='+keyword+'&key=AIzaSyBE1vzq5FFFCn688oul-g1uFQOyWJhiWJY';

    //make the API call:
    $.ajax({
        type: "GET",
        url: yt_url,
        async: false,
        dataType:"jsonp",
        success: function(response){
            //console.log(response);

            //if we get videos back:
            if(response.items){

                //loop through each video result and populate the results container
                $.each(response.items, function(i,data){

                    //extract the parts from the results set we are going to use:
                    var video_id = data.id.videoId;
                    var video_title = data.snippet.title;
                    var video_thumb = data.snippet.thumbnails.medium.url;

                    var list_item="<li class='normal' data-id='"+video_id+"'>"+video_title+" <img src='"+video_thumb+"'/></li>";
                    
                    //clone template to create new dom element. inject template, then insert into dom.
                    var $thisResult = $('#youtube-resultTemplate').clone();
                    $('#youtube-resultsList').append(list_item);
                });
            }
        }
    });
}