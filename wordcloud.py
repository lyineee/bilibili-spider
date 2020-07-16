from wordcloud import WordCloud
from analysis import BiliData
from PIL import Image

if __name__ == "__main__":
    year=2011
    db=BiliData('localhost',27017)
    data=db.get_word_dict(year,50)
    font = r'C:\Windows\Fonts\simfang.ttf'
    wc=WordCloud(width=2800, height=1400,font_path=font,background_color='white')
    a=wc.generate_from_frequencies(data)
    img=a.to_image
    a.to_file(str(year)+'.png')

