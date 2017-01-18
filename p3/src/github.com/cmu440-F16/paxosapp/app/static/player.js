// 2. This code loads the IFrame Player API code asynchronously.
var tag = document.createElement('script');
tag.src = "https://www.youtube.com/iframe_api";
var firstScriptTag = document.getElementsByTagName('script')[0];
firstScriptTag.parentNode.insertBefore(tag, firstScriptTag);
// 3. This function creates an <iframe> (and YouTube player)
//    after the API code downloads.
var player;

var currentIndex = 0
var playlist = []
var waitingForNext = false

function onYouTubeIframeAPIReady() {
    player = new YT.Player('player', {
        height: '390',
        width: '640',
        events: {
            'onReady': onPlayerReady,
            'onStateChange': onPlayerStateChange
        }
    });
}
// 4. The API will call this function when the video player is ready.
function onPlayerReady(event) {
    var chat = new WebSocket("ws://"+window.location.host+"/ws")
    chat.onmessage = receiveMessage
    chat.onerror = displayError
    chat.onclose = displayError
}

function onPlayerStateChange(event) {
    if (event.data == 0) { // video ended
        currentIndex++
        if (playlist.length <= currentIndex) {
            waitingForNext = true
        } else {
            loadNextVideo()
        }
    }
}

function receiveMessage(evt) {
    data = JSON.parse(evt.data)
    playlist.push(data.id)
    $("ol").append("<li>"+data.title+"</li>")
    if (playlist.length == 1) {
        $("#novideosyet").hide()
        player.loadVideoById(data.id)
        $("ol li:nth-child(1)").css("color", "red")
    } else if (waitingForNext) {
        loadNextVideo()
        waitingForNext = false
    }
}

function loadNextVideo(){
    //get id for next video
    player.loadVideoById(playlist[currentIndex])

    // make old one black
    var query = "ol li:nth-child("+currentIndex+")"
    $(query).css("color", "black")

    // make current one red
    var oneOrderedIndex = currentIndex+1
    query = "ol li:nth-child("+oneOrderedIndex+")"
    $(query).css("color", "red")
}

function displayError(event) {
    window.alert("Connection lost. Please refresh the page once the server is up.")
}

$("#playlist").on('click', 'li', function() {
    var newIndex = $(this).index()
    player.loadVideoById(playlist[newIndex])

    // make old one black
    var oldIndex = currentIndex+1
    var query = "ol li:nth-child("+oldIndex+")"
    $(query).css("color", "black")

    // make current one red
    var oneOrderedIndex = newIndex+1
    query = "ol li:nth-child("+oneOrderedIndex+")"
    $(query).css("color", "red")

    currentIndex = newIndex
});